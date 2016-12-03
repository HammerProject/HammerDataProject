curl "http://localhost:8983/solr/openafrica/query?start=0&rows=100" -d '
{
  "query" : "women emergency",
  "fields" : ["title"]
}'

