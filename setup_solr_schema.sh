#!/bin/bash

# Script to set up Solr schema for StormCrawler
# Run this after your Solr container is up and running

SOLR_URL="http://127.0.0.1:8983/solr"
CORE_NAME="crawler"

echo "Setting up Solr schema for StormCrawler..."

# Add basic field types if they don't exist
echo "Adding field types..."

# Text field type for content
curl -X POST -H 'Content-type:application/json' \
  --data-binary '{
    "add-field-type": {
      "name": "text_general",
      "class": "solr.TextField",
      "positionIncrementGap": "100",
      "analyzer": {
        "tokenizer": {
          "class": "solr.StandardTokenizerFactory"
        },
        "filters": [
          {"class": "solr.LowerCaseFilterFactory"},
          {"class": "solr.StopFilterFactory", "ignoreCase": "true", "words": "stopwords.txt"},
          {"class": "solr.SynonymGraphFilterFactory", "synonyms": "synonyms.txt", "ignoreCase": "true", "expand": "true"}
        ]
      }
    }
  }' \
  ${SOLR_URL}/${CORE_NAME}/schema

# Add required fields for StormCrawler
echo "Adding StormCrawler fields..."

# ID field (URL)
curl -X POST -H 'Content-type:application/json' \
  --data-binary '{
    "add-field": {
      "name": "id",
      "type": "string",
      "indexed": true,
      "stored": true,
      "required": true
    }
  }' \
  ${SOLR_URL}/${CORE_NAME}/schema

# Title field
curl -X POST -H 'Content-type:application/json' \
  --data-binary '{
    "add-field": {
      "name": "title",
      "type": "text_general",
      "indexed": true,
      "stored": true
    }
  }' \
  ${SOLR_URL}/${CORE_NAME}/schema

# Text content field
curl -X POST -H 'Content-type:application/json' \
  --data-binary '{
    "add-field": {
      "name": "text",
      "type": "text_general",
      "indexed": true,
      "stored": true
    }
  }' \
  ${SOLR_URL}/${CORE_NAME}/schema

# Host field
curl -X POST -H 'Content-type:application/json' \
  --data-binary '{
    "add-field": {
      "name": "host",
      "type": "string",
      "indexed": true,
      "stored": true
    }
  }' \
  ${SOLR_URL}/${CORE_NAME}/schema

# Description field
curl -X POST -H 'Content-type:application/json' \
  --data-binary '{
    "add-field": {
      "name": "description",
      "type": "text_general",
      "indexed": true,
      "stored": true
    }
  }' \
  ${SOLR_URL}/${CORE_NAME}/schema

# Keywords field (tokenized text)
curl -X POST -H 'Content-type:application/json' \
  --data-binary '{
    "add-field": {
      "name": "keywords_t",
      "type": "text_general",
      "indexed": true,
      "stored": true,
      "multiValued": true
    }
  }' \
  ${SOLR_URL}/${CORE_NAME}/schema

# URL field (stored separately for display)
curl -X POST -H 'Content-type:application/json' \
  --data-binary '{
    "add-field": {
      "name": "url",
      "type": "string",
      "indexed": false,
      "stored": true
    }
  }' \
  ${SOLR_URL}/${CORE_NAME}/schema

# Timestamp field for crawl date
curl -X POST -H 'Content-type:application/json' \
  --data-binary '{
    "add-field": {
      "name": "crawl_date",
      "type": "pdate",
      "indexed": true,
      "stored": true
    }
  }' \
  ${SOLR_URL}/${CORE_NAME}/schema

# Content type field
curl -X POST -H 'Content-type:application/json' \
  --data-binary '{
    "add-field": {
      "name": "content_type",
      "type": "string",
      "indexed": true,
      "stored": true
    }
  }' \
  ${SOLR_URL}/${CORE_NAME}/schema

# Language field
curl -X POST -H 'Content-type:application/json' \
  --data-binary '{
    "add-field": {
      "name": "lang",
      "type": "string",
      "indexed": true,
      "stored": true
    }
  }' \
  ${SOLR_URL}/${CORE_NAME}/schema

echo "Schema setup complete!"
echo "You can verify the schema at: ${SOLR_URL}/${CORE_NAME}/schema"

# Test the core
echo "Testing core connectivity..."
curl -s "${SOLR_URL}/${CORE_NAME}/admin/ping" | grep -q '"status":"OK"' && echo "✅ Core is accessible" || echo "❌ Core test failed"
