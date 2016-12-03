curl "http://localhost:8983/solr/openafrica/query?start=0&rows=100" -d '
{
  "query" : "year month spouse age 35 civilunions 2012",
  "fields" : ["title"]
}'
