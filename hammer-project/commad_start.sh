#for test env

bin/yarn jar share/test/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App datasource
bin/yarn jar share/test/hammer-pinta-0.0.2.jar org.hammer.pinta.App


#for prod env

bin/yarn jar share/hammer/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App datasource
bin/yarn jar share/hammer/hammer-pinta-0.0.2.jar org.hammer.pinta.App
