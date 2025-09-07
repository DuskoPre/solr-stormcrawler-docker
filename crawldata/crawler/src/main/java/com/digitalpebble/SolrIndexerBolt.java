package com.digitalpebble;

import java.io.IOException;
import java.util.*;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;

public class SolrIndexerBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(SolrIndexerBolt.class);

    private OutputCollector collector;
    private SolrClient solrClient;
    private String solrUrl;
    private int commitSize = 250;
    private int docCount = 0;
    private List<SolrInputDocument> batch;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.batch = new ArrayList<>();
        
        // Get Solr configuration
        this.solrUrl = ConfUtils.getString(conf, "solr.url", "http://localhost:8983/solr/crawler");
        this.commitSize = ConfUtils.getInt(conf, "solr.commit.size", 250);
        
        LOG.info("Connecting to Solr at: {}", solrUrl);
        
        // Initialize Solr client
        this.solrClient = new Http2SolrClient.Builder(solrUrl).build();
        
        try {
            // Test connection
            solrClient.ping();
            LOG.info("Successfully connected to Solr");
        } catch (Exception e) {
            LOG.error("Failed to connect to Solr: {}", e.getMessage(), e);
            throw new RuntimeException("Cannot connect to Solr", e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        byte[] content = tuple.getBinaryByField("content");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        
        if (content == null || content.length == 0) {
            LOG.debug("Empty content for URL: {}", url);
            collector.ack(tuple);
            return;
        }
        
        try {
            // Create Solr document
            SolrInputDocument doc = new SolrInputDocument();
            
            // Add basic fields
            doc.addField("id", url);
            doc.addField("url", url);
            doc.addField("text", new String(content, "UTF-8"));
            doc.addField("crawl_date", new Date());
            
            // Add metadata fields
            if (metadata != null) {
                // Title
                String[] titles = metadata.getValues("parse.title");
                if (titles != null && titles.length > 0) {
                    doc.addField("title", titles[0]);
                }
                
                // Description
                String[] descriptions = metadata.getValues("parse.description");
                if (descriptions != null && descriptions.length > 0) {
                    doc.addField("description", descriptions[0]);
                }
                
                // Keywords
                String[] keywords = metadata.getValues("parse.keywords");
                if (keywords != null && keywords.length > 0) {
                    doc.addField("keywords_t", Arrays.asList(keywords));
                }
                
                // Content type
                String[] contentTypes = metadata.getValues("Content-Type");
                if (contentTypes != null && contentTypes.length > 0) {
                    doc.addField("content_type", contentTypes[0]);
                }
                
                // Host
                try {
                    java.net.URL parsedUrl = new java.net.URL(url);
                    doc.addField("host", parsedUrl.getHost());
                } catch (Exception e) {
                    LOG.debug("Failed to parse host from URL: {}", url);
                }
                
                // Language
                String[] languages = metadata.getValues("parse.lang");
                if (languages != null && languages.length > 0) {
                    doc.addField("lang", languages[0]);
                }
            }
            
            // Add to batch
            batch.add(doc);
            docCount++;
            
            // Check if we should commit
            if (docCount >= commitSize) {
                commitBatch();
            }
            
            // Emit status update for successful indexing
            collector.emit(Constants.StatusStreamName, tuple, 
                new Values(url, metadata, Status.FETCHED));
            collector.ack(tuple);
            
        } catch (Exception e) {
            LOG.error("Failed to index document: {}", url, e);
            
            // Emit error status
            collector.emit(Constants.StatusStreamName, tuple, 
                new Values(url, metadata, Status.ERROR));
            collector.fail(tuple);
        }
    }
    
    private void commitBatch() {
        if (batch.isEmpty()) {
            return;
        }
        
        try {
            LOG.info("Committing batch of {} documents to Solr", batch.size());
            solrClient.add(batch);
            solrClient.commit();
            
            batch.clear();
            docCount = 0;
            
            LOG.info("Successfully committed batch to Solr");
        } catch (Exception e) {
            LOG.error("Failed to commit batch to Solr", e);
            // Don't clear batch on error - will retry
        }
    }

    @Override
    public void cleanup() {
        // Commit any remaining documents
        if (!batch.isEmpty()) {
            commitBatch();
        }
        
        // Close Solr client
        if (solrClient != null) {
            try {
                solrClient.close();
            } catch (IOException e) {
                LOG.error("Error closing Solr client", e);
            }
        }
        
        super.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constants.StatusStreamName, new Fields("url", "metadata", "status"));
    }
}
