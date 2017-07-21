###############################################
# script and utils to work with apache solr
# solr

bin/solr start -e cloud -noprompt


bin/solr create -c openafrica

bin/solr delete -c openafrica


bin/solr create -c openafricaj


http://ma-ha-2:8983/solr/#/

## delele all documents
curl http://ma-ha-2:8983/solr/cityofnewyork/update -H "Content-Type: text/xml" --data-binary '<delete><query>*:*</query></delete>'


bin/post -c openafrica ../../africa_org/festival_mantova.json

bin/post -c openafricaj ../../africa_org/*.json


curl "http://localhost:8983/solr/openafrica/update/extract?literal.id=festival_mantova.json&commit=true" -F "myfile=@../../africa_org/festival_mantova.json"


## create cityofnewyork
bin/solr create -c cityofnewyorkj
bin/post -c cityofnewyorkj ../../cityofnewyork/*.json

