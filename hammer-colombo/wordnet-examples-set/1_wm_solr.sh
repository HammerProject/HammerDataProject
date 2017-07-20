curl "http://localhost:8983/solr/cityofnewyork/query?start=0&rows=100" -d '
{
  "query" : "fountains location site address drinking",
  "fields" : ["title"]
}'
