curl "http://localhost:8983/solr/openafrica/query?start=0&rows=100" -d '
{
  "query" : "national exports Orchids",
  "fields" : ["title"]
}'

