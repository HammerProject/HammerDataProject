###############################################
# script and utils to work with apache solr
# solr

bin/solr start -e cloud -noprompt


http://ma-ha-2:8983/solr/#/

## delele all documents
curl http://ma-ha-2:8983/solr/gettingstarted/update -H "Content-Type: text/xml" --data-binary '<delete><query>*:*</query></delete>'


bin/post -c gettingstarted ../../africa_org/festival_mantova.json

bin/post -c gettingstarted ../../africa_org/*.json
