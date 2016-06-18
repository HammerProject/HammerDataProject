###############################################################################
####### TEST ENV ##############################################################

## start santamaria with open sources from "datasource1" collection
bin/yarn jar share/test/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App datasource1

## start twitter stream for "lombardia" box
bin/yarn jar share/test/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App tweets lombardia

## create/update the inverted index
bin/yarn jar share/test/hammer-pinta-0.0.2.jar org.hammer.pinta.App

## download all resources by query
bin/yarn jar share/test/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/test/query_1.json local download labels 0.0 0.0

## download selected resources by query
bin/yarn jar share/test/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/test/example_1.json local download keywords 0.0 0.0

## search all resources by query
bin/yarn jar share/test/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/test/example_1.json local search labels 0.3 0.3

## search selected resources by query
bin/yarn jar share/test/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/test/example_1f.json local search keywords 0.3 0.3


###############################################################################
####### PROD ENV ##############################################################


## start santamaria with open sources from "datasource" collection
bin/yarn jar share/hammer/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App datasource

## start twitter stream for "lombardia" box
bin/yarn jar share/hammer/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App tweets lombardia

## create/update the inverted index
bin/yarn jar share/hammer/hammer-pinta-0.0.2.jar org.hammer.pinta.App


## download all resources by query
bin/yarn jar share/hammer/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/hammer/query_1.json local download labels 0.0 0.0

## download selected resources by query
bin/yarn jar share/hammer/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/hammer/example_1.json local download keywords 0.0 0.0

## search all resources by query
bin/yarn jar share/hammer/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/hammer/example_1.json local search labels 0.3 0.3

## search selected resources by query
bin/yarn jar share/hammer/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/hammer/example_1.json local search keywords 0.3 0.3



##################################################################################
####### OTHER UTILS ##############################################################


## check size of folder
bin/hadoop fs -du -s /hammer/download

## purge download folder
bin/hadoop fs -rm -r /hammer/download

## create download folder
bin/hadoop fs -mkdir /hammer/download