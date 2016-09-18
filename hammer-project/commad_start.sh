###############################################################################
####### TEST ENV ##############################################################

## start santamaria with open sources from "datasource1" collection
bin/yarn jar share/test/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App datasource
bin/yarn jar share/test/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App datasource1
bin/yarn jar share/test/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App datasource2

## start twitter stream for "lombardia" box
bin/yarn jar share/test/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App tweets lombardia

## create/update the inverted index and calc re/sim of each tems
bin/yarn jar share/test/hammer-pinta-0.0.2.jar org.hammer.pinta.App 0.95 true 5 subset subindex

## create/update the inverted index only calc re/sim of each tems
bin/yarn jar share/test/hammer-pinta-0.0.2.jar org.hammer.pinta.App 0.95 false 5 subset subindex


## download all resources by query
bin/yarn jar share/test/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/test/query_1.json local download labels 0.0 0.0 0.0 0 dataset index 0.0

## download selected resources by query
bin/yarn jar share/test/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/test/example_1.json local download keywords 0.0 0.0 0.0 0 dataset index 0.0

## search all resources by query
bin/yarn jar share/test/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/test/example_1.json local search labels 0.3 0.3 0.90 2 subset subindex 0.9996

## search selected resources by query
bin/yarn jar share/test/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/test/test_1f.json local search keywords 0.3 0.3 0.90 2 subset subindex 0.9996


###############################################################################
####### PROD ENV ##############################################################


## start santamaria with open sources from "datasource" collection
bin/yarn jar share/hammer/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App datasource

## start twitter stream for "lombardia" box
bin/yarn jar share/hammer/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App tweets lombardia

## create/update the inverted index and calc re/sim of each tems
bin/yarn jar share/hammer/hammer-pinta-0.0.2.jar org.hammer.pinta.App 0.95 true 5 dataset index

## create/update the inverted index only calc re/sim of each tems
bin/yarn jar share/hammer/hammer-pinta-0.0.2.jar org.hammer.pinta.App 0.95 false 5 dataset index



## download all resources by query
bin/yarn jar share/hammer/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/hammer/query_1.json local download labels 0.0 0.0 0.0 0 dataset index 0.0

## download selected resources by query
bin/yarn jar share/hammer/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/hammer/example_1.json local download keywords 0.0 0.0 0.0 0 dataset index 0.0

## search all resources by query
bin/yarn jar share/hammer/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/hammer/example_1.json local search labels 0.3 0.3 0.90 2 dataset index 0.9996

## search selected resources by query
bin/yarn jar share/hammer/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/hammer/example_1.json local search keywords 0.3 0.3 0.90 2 dataset index 0.9996


##################################################################################
####### OTHER UTILS ##############################################################


## check size of folder
bin/hadoop fs -du -s /hammer/download

## purge download folder
bin/hadoop fs -rm -r /hammer/download

## create download folder
bin/hadoop fs -mkdir /hammer/download