package com.digitalpebble;

import com.digitalpebble.stormcrawler.*;
import com.digitalpebble.stormcrawler.bolt.FetcherBolt;
import com.digitalpebble.stormcrawler.bolt.JSoupParserBolt;
import com.digitalpebble.stormcrawler.bolt.SiteMapParserBolt;
import com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt;
import com.digitalpebble.stormcrawler.bolt.FeedParserBolt;
import com.digitalpebble.SolrIndexerBolt;
import com.digitalpebble.stormcrawler.urlfrontier.Spout;
import com.digitalpebble.stormcrawler.urlfrontier.StatusUpdaterBolt;
import com.digitalpebble.stormcrawler.tika.ParserBolt;
import com.digitalpebble.stormcrawler.tika.RedirectionBolt;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.Config;

import static com.digitalpebble.stormcrawler.Constants.StatusStreamName;

/**
 * Crawler topology with Solr indexing
 */
public class CrawlTopology extends ConfigurableTopology {

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new CrawlTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        // HTTP agent configuration
        conf.put("http.agent.name", "TestCrawler");
        conf.put("http.agent.version", "1.0");
        conf.put("http.agent.description", "Test crawler built with StormCrawler");
        conf.put("http.agent.url", "http://example.com/");
        conf.put("http.agent.email", "test@example.com");
        conf.put("http.protocol.implementation", "com.digitalpebble.stormcrawler.protocol.okhttp.HttpProtocol");
        conf.put("https.protocol.implementation", "com.digitalpebble.stormcrawler.protocol.okhttp.HttpProtocol");
        conf.put("http.content.limit", 65536);
        
        // URLFrontier configuration
        conf.put("urlfrontier.host", "frontier");
        conf.put("urlfrontier.port", 7071);
        
        // Fetch intervals
        conf.put("fetchInterval.default", 1440);
        conf.put("fetchInterval.fetch.error", 120);
        conf.put("fetchInterval.error", -1);
        conf.put("scheduler.class", "com.digitalpebble.stormcrawler.persistence.DefaultScheduler");

        // Solr configuration - using Docker container name
        conf.put("solr.url", "http://solr-neural-search-solr-1:8983/solr/crawler");
        conf.put("solr.commit.size", 250);
        conf.put("solr.max.docs.batch", 100);

        builder.setSpout("spout", new Spout());

        builder.setBolt("partitioner", new URLPartitionerBolt()).shuffleGrouping("spout");

        builder.setBolt("fetch", new FetcherBolt()).fieldsGrouping("partitioner", new Fields("key"));

        builder.setBolt("sitemap", new SiteMapParserBolt()).localOrShuffleGrouping("fetch");

        builder.setBolt("feeds", new FeedParserBolt()).localOrShuffleGrouping("sitemap");

        builder.setBolt("parse", new JSoupParserBolt()).localOrShuffleGrouping("feeds");

        builder.setBolt("shunt", new RedirectionBolt()).localOrShuffleGrouping("parse");

        builder.setBolt("tika", new ParserBolt()).localOrShuffleGrouping("shunt", "tika");

        // Use custom Solr indexer instead of StdOut
        builder.setBolt("index", new SolrIndexerBolt())
                .localOrShuffleGrouping("shunt")
                .localOrShuffleGrouping("tika");

        Fields furl = new Fields("url");

        builder.setBolt("status", new StatusUpdaterBolt())
                .fieldsGrouping("fetch", Constants.StatusStreamName, furl)
                .fieldsGrouping("sitemap", Constants.StatusStreamName, furl)
                .fieldsGrouping("feeds", Constants.StatusStreamName, furl)
                .fieldsGrouping("parse", Constants.StatusStreamName, furl)
                .fieldsGrouping("tika", Constants.StatusStreamName, furl)
                .fieldsGrouping("index", Constants.StatusStreamName, furl);

        return submit("crawl", conf, builder);
    }
}
