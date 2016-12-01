### run elastic search

bin/elasticsearch


### check

curl http://localhost:9200/


### check index

curl http://localhost:9200/_cat/indices?v

### create a new index

curl -X PUT http://localhost:9200/opendata


### bulk load
curl -XPUT 'localhost:9200/opendata/1' -d'
{
    "user" : "mauro",
    "post_date" : "2016-11-30T06:20:00",
    "message" : "trying out Elasticsearch"
}'

curl -XPUT 'localhost:9200/opendata/2' -d'
{
    "user" : "paola",
    "post_date" : "2016-11-30T06:20:00",
    "name" : "Paola Ravasio",
    "message" : "some message here"
}'


curl -XPUT 'localhost:9200/opendata/3' -d'
{
    "user" : "nicola",
    "post_date" : "2016-11-30T06:20:00",
    "name" : "Nik Nicola",
    "education" : "Unimib",
    "message" : "my new message"
}'


curl -XPOST 'localhost:9200/opendata/' -d'
{
    "user" : "mario",
    "post_date" : "2016-11-30T06:20:00",
    "name" : "Mario Rossi",
    "education" : "Unimib",
    "message" : "i have a generated-id"
}'



curl -XPOST 'localhost:9200/opendata/_search' -d'
{
  "query": {
    "match": {
      "name": {
        "query": "message",
        "fuzziness": 4,
        "prefix_length": 1
      }
    }
  }
}'




# Create the index for fuzzy search
curl -XPUT 'localhost:9200/fuzzy_products'

# Create the product mapping
curl -XPUT 'localhost:9200/fuzzy_products/product/_mapping' -d'
{
  "product": {
    "properties": {
      "name": {
        "type": "string",
        "analyzer": "simple"
      }
    }
  }
}'

# Upload some documents
curl -XPUT 'localhost:9200/fuzzy_products/product/1' -d'
{"name": "Vacuum Cleaner"}'

curl -XPUT 'localhost:9200/fuzzy_products/product/2' -d'
{"name": "Turkey Baster"}'


# Perform a fuzzy search!
curl -XPUT 'localhost:9200/fuzzy_products/product/_search' -d'
{
  "query": {
    "match": {
      "name": {
        "query": "Vacuummm",
        "fuzziness": 2,
        "prefix_length": 1
      }
    }
  }
}'




### upload json file
cat ../africa_org/festival_mantova.json | ./jq-linux64 -c '.[] | {"index": {"_index": "africa_opendata", "_type": "africa_opendata"}}, .' | curl -XPOST localhost:9200/africa_opendata/_bulk --data-binary @-

cat ../africa_org/1_fuzzy_b.json | ./jq-linux64 -c '.[] | {"index": {"_index": "africa_opendata", "_type": "africa_opendata"}}, .' | curl -XPOST localhost:9200/africa_opendata/_bulk --data-binary @-


