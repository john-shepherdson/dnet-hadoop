#CREATE SCHOLIX INDEX
curl -XPUT 'localhost:9200/scholix_shadow?pretty' -H 'Content-Type: application/json' -d'{
  "mappings":{
    "properties":{
      "identifier":{
        "type":"keyword"
      },
      "linkProviders":{
        "type":"text"
      },
      "publicationDate":{
        "type":"date"
      },
      "relationType":{
        "type":"keyword"
      },
      "sourceId":{
        "type":"keyword"
      },
      "sourcePid":{
        "type":"keyword"
      },
      "sourcePidType":{
        "type":"keyword"
      },
      "sourcePublisher":{
        "type":"text"
      },
      "sourceSubType":{
        "type":"keyword"
      },
      "sourceType":{
        "type":"keyword"
      },
      "targetId":{
        "type":"keyword"
      },
      "targetPid":{
        "type":"keyword"
      },
      "targetPidType":{
        "type":"keyword"
      },
      "targetPublisher":{
        "type":"text"
      },
      "targetSubType":{
        "type":"keyword"
      },
      "targetType":{
        "type":"keyword"
      }
    }
  },
  "settings": {
    "index": {
      "refresh_interval":"36000s",
      "number_of_shards":"100",
      "translog":{
        "sync_interval":"15s",
        "durability":"ASYNC"
      },
      "analysis": {
        "analyzer": {
          "analyzer_keyword": {
            "filter": "lowercase",
            "tokenizer": "keyword"
          }
        }
      },
      "number_of_replicas": "0"
    }
  }
}'
#CREATE SUMMARY INDEX
curl -XPUT 'localhost:9200/summary_shadow?pretty' -H 'Content-Type: application/json' -d'
{
  "mappings":{
    "properties":{
      "body":{
        "type":"text",
        "index":false
      },
      "objectType":{
        "type":"keyword"
      }
    }
  },
  "settings": {
    "index": {
      "refresh_interval":"36000s",
      "number_of_shards":"30",
      "translog": {
        "sync_interval": "15s",
        "durability": "ASYNC"
      },
      "analysis": {
        "analyzer": {
          "analyzer_keyword": {
            "filter": "lowercase",
            "tokenizer": "keyword"
          }
        }
      },
      "number_of_replicas": "0"
    }
  }
}'

#CREATE ALIASES
curl -XPOST 'localhost:9200/_aliases' -H 'Content-Type: application/json' -d'
{
  "actions": [
    {
      "remove": {
        "index": "scholix",
        "alias": "scholix_current"
      }
    }
  ]
}'

#SWITCH ALIASES SCHOLIX
curl -XPOST 'localhost:9200/_aliases' -H 'Content-Type: application/json' -d'
{
  "actions": [
    {
      "remove": {
        "index": "scholix_shadow",
        "alias": "scholix_current"
      }
    },
    {
      "add": {
        "index": "scholix",
        "alias": "scholix_current"
      }
    }
  ]
}'
#SWITCH ALIASES SUMMARY
curl -XPOST 'localhost:9200/_aliases' -H 'Content-Type: application/json' -d'
{
  "actions": [
    {
      "remove": {
        "index": "summary_shadow",
        "alias": "summary_current"
      }
    },
    {
      "add": {
        "index": "summary",
        "alias": "summary_current"
      }
    }
  ]
}'