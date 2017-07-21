curl "http://localhost:8983/solr/cityofnewyork/query?start=0&rows=100" -d '
{
  "query" : "payroll basesalary regularhour title fiscalyear 2016",
  "fields" : ["dataset_description title name.description"]
}'
