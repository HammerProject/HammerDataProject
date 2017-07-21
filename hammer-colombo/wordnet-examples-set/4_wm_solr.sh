curl "http://localhost:8983/solr/cityofnewyork/query?start=0&rows=100" -d '
{
  "query" : "fountains property site address drinking borough m",
  "fields" : ["dataset_description title"]
}'
