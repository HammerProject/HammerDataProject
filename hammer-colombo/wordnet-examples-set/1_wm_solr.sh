curl "http://localhost:8983/solr/cityofnewyork/query?start=0&rows=100" -d '
{
  "query" : "mountains scultpor park location architect Frederic Auguste Bartholdi",
  "fields" : ["title"]
}'
