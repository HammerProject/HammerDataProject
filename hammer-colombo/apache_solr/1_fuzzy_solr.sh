curl http://localhost:8983/solr/gettingstarted/query -d '
{
  "query" : "Nutrition~2 Monbasa~2 Turkana~2 Nairobi~2",
  "fields" : ["*county*","*underweight*","*stuting*","*wasting*"]
}'


curl http://localhost:8983/solr/openafrica/query -d '
{
  "query" : "mantova~2",
  "fields" : ["*filename*"]
}'