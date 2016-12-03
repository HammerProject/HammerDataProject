curl "http://localhost:8983/solr/openafrica/query?start=0&rows=100" -d '
{
  "query" : "wasting stuting underweight Nutrition County Monbasa Turkana Nairobi",
  "fields" : ["title"]
}'


curl "http://localhost:8983/solr/openafricaj/query?start=0&rows=10" -d '
{
  "query" : "(wasting stuting underweight Nutrition) and (County=Monbasa or county=Turkana or county=Nairobi)",
  "fields" : ["id","stuting*","underweight*","*county*","*"]
}'
