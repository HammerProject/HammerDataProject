# Hammer Project: Get your open data!
#
last update 27/3/2016

**Hammer Project** allows queries on unstructured data. Particularly on **OPEN DATA**!

From a JSON-like query, you can extract data in JSON format.

Hammer is based on **Hadoop** and **YARN**.

# Module
Hammer is compose by four module:

1. SantaMaria: the crawler that create a catalog of Open Data);
2. Pinta: the indexer (from catalog it creates an inverted-index);
3. Isabella: the parser for the JSON-like query language;
4. Colombo: the search engine;
5. Taino: the front-end.

For repository Hammer use **MongoDB**

# Isabella Query Language

Isabella is a JSON-like Query Language.

The SQL query:
```sql
SELECT sito, comune, inquinamento FROM contaminati WHERE provincia = 'Bergamo'
```
in Isabella is equivalent to
```json
{
	"select": [
		{ "column": { "instance": "contaminati", "label":"sito"}},
		{ "column": { "instance": "contaminati", "label":"comune"}},
		{ "column": { "instance": "contaminati", "label":"inquinamento"}},
	],
	"from": [
		"contaminati"
	],
	"where": [
		{"condition": {
			"instance1": "contaminati",
			"label1": "provincia",
			"operator": "eq",
			"value": "BERGAMO"
			}
		}
	]
}
```


Another example:
```json
{
  "select": [
      { "column": { "instance": "aria", "label":"CO_MEDIA_MOBILE_MASSIMA_GIORNALIERA_mg_m3"}},
      { "column": { "instance": "aria", "label":"CO_MEDIA_MOBILE_MASSIMA_GIORNALIERA_mg_m3"}},
      { "column": { "instance": "aria", "label":"CO_MEDIA_MOBILE_MASSIMA_GIORNALIERA_mg_m3"}},
      { "column": { "instance": "aria", "label":"CO_SUPERAMENTIMEDIA_MOBILE_8_ORE"}},
      { "column": { "instance": "aria", "label":"ANAGRAFICA_STAZIONE_DI_CAMPIONAMENTO_PROVINCIA"}},
      { "column": { "instance": "aria", "label":"ANAGRAFICA_STAZIONE_DI_CAMPIONAMENTO_PROVINCIA"}},
      { "column": { "instance": "cened", "label":"emissioni_di_co2"}},
      { "column": { "instance": "cened", "label":"comune"}},
      { "column": { "instance": "polizia", "label":"num_sanz_ambientali"}},
      { "column": { "instance": "polizia", "label":"ente"}}
  ],
  "from": [
    "cened",
    "aria",
    "polizia"
  ],
  "where": [{"condition": {
      "instance1": "aria",
      "label1": "pm10_media_annua_g_m3",
      "operator": "gt",
      "value": 0,
      "logicalOperator": "and"
    }},{"condition": {
      "instance1": "cened",
      "label1": "emissioni_di_co2",
      "operator": "gt",
      "value": 0,
      "logicalOperator": "and"
    }},{"condition": {
      "instance1": "polizia",
      "label1": "num_sanz_ambientali",
      "operator": "gt",
      "value": 0,
      "logicalOperator": "and"
    }},{"condition": {
      "instance1": "cened",
      "label1": "tipologia_combustibile",
      "operator": "eq",
      "value": "Gasolio",
      "logicalOperator": "and"
    }}
  ]
}
```

# Use

After download the source code, you must configure every YARN application.


For the module SantaMaria, the config is in class ```org.hammer.santamaria.SantaMariaConfig```:

```
public SantaMariaConfig(final Configuration conf) {
        setConf(conf);
        MongoConfigUtil.setInputFormat(conf, SantaMariaInputFormat.class);
        MongoConfigUtil.setOutputFormat(conf, SantaMariaOutputFormat.class);
        
        
        MongoConfigUtil.setMapper(conf, SantaMariaMapper.class);
        MongoConfigUtil.setMapperOutputKey(conf, Text.class);
        MongoConfigUtil.setMapperOutputValue(conf, BSONWritable.class);

        MongoConfigUtil.setReducer(conf, SantaMariaReducer.class);
        MongoConfigUtil.setOutputKey(conf, Text.class);
        MongoConfigUtil.setOutputValue(conf, BSONWritable.class);
        
        
        MongoConfigUtil.setInputURI(conf, "mongodb://192.168.56.90:27017/hammer.datasource");
        MongoConfigUtil.setOutputURI(conf, "mongodb://192.168.56.90:27017/hammer.dataset");
        
    }
```

Change the host/port (```mongodb://192.168.56.90:27017/hammer...```)  with your db instance.

After compile the source code, put the ```hammer-santamaria-0.0.2.jar``` to ```share\hammer``` folder in your HADOOP_HOME

To run SantaMaria, from your HADOOP_HOME digit
```
bin/yarn jar share/hammer/hammer-santamaria-0.0.2.jar org.hammer.santamaria.App
```


For the module Pinta, the configuration is in class ```org.hammer.pinta.PintaConfig```:
```
public PintaConfig(final Configuration conf) {
        setConf(conf);
        MongoConfigUtil.setInputFormat(conf, PintaInputFormat.class);
        MongoConfigUtil.setOutputFormat(conf, PintaOutputFormat.class);
        
        
        MongoConfigUtil.setMapper(conf, PintaMapper.class);
        MongoConfigUtil.setMapperOutputKey(conf, Text.class);
        MongoConfigUtil.setMapperOutputValue(conf, BSONWritable.class);

        MongoConfigUtil.setReducer(conf, PintaReducer.class);
        MongoConfigUtil.setOutputKey(conf, Text.class);
        MongoConfigUtil.setOutputValue(conf, BSONWritable.class);

        MongoConfigUtil.setInputURI(conf, "mongodb://192.168.56.90:27017/hammer.dataset");
        MongoConfigUtil.setOutputURI(conf, "mongodb://192.168.56.90:27017/hammer.index");

    }
```
Change the host/port (```mongodb://192.168.56.90:27017/hammer....``` )  with your db instance.

After compile the source code, put the ```hammer-pinta-0.0.2.jar``` to ```share\hammer``` folder in your HADOOP_HOME

To run Pinta and create the inverted index, from your HADOOP_HOME digit
```
bin/yarn jar share/hammer/hammer-pinta-0.0.2.jar org.hammer.pinta.App
```
If you want use a thesaurus you must also edit the App class

```
public static void Run() throws Exception {
		...
		conf.set("thesaurus.url", "http://thesaurus.altervista.org/thesaurus/v1"); // the thesaurus url
		conf.set("thesaurus.key", "bVKAPIcUum3hEFGKEBAu"); // your key
		conf.set("thesaurus.lang", "it_IT"); // the language
		...
	}
```



For the module Colombo (the search engine), the configuration is in class ```org.hammer.colombo.ColomboConfig```:
```
public ColomboConfig(final Configuration conf) {
        setConf(conf);
        MongoConfigUtil.setInputFormat(conf, ColomboInputFormat.class);
        MongoConfigUtil.setOutputFormat(conf, ColomboOutputFormat.class);
        ...
        MongoConfigUtil.setInputURI(conf, "mongodb://192.168.56.90:27017/hammer.dataset");
        MongoConfigUtil.setOutputURI(conf, "mongodb://192.168.56.90:27017/hammer." + conf.get("query-table"));
	...
    }
```
Change the host/port (```mongodb://192.168.56.90:27017/hammer....``` )  with your db instance.


You also edit the App file:
```
public static void Run(String fileQuery, String mode, float limit) throws Exception {
		System.out.println("!!! Hammer Project !!!");
		System.out.println("!!! Colombo Module start.....");
		Configuration conf = new Configuration();
		...
		conf.set("thesaurus.url", "http://thesaurus.altervista.org/thesaurus/v1"); // the thesaurus url
		conf.set("thesaurus.key", "bVKAPIcUum3hEFGKEBAu"); // your key
		conf.set("thesaurus.lang", "it_IT"); // the language
		...	
		// insert a limit to socrata recordset for memory head problem, deprecated
		conf.set("socrata.record.limit", "30000"); // the limit of result for each document
		...
		...
		...
		// if choose hdfs file system, configure the entry point
		if(mode.equals("hdfs")) {
			conf.set("query-file", "hdfs://192.168.56.90:9000/hammer/" + fileQuery);
			query = ReadFileFromHdfs(conf);
		} else {
			conf.set("query-file", fileQuery);
			query = IsabellaUtils.readFile(fileQuery);
		}
		...
	}
```


After compile the source code, put the ```hammer-cololmbo-0.0.2.jar``` to ```share\hammer``` folder in your HADOOP_HOME

To run Colombo, from your HADOOP_HOME digit
```
bin/yarn jar share/hammer/hammer-colombo-0.0.2.jar org.hammer.colombo.App share/hammer/your_query.json local 0.0
```

Colombo App takes three parameter:

1. ```share/hammer/your_query.json```: the path of the file with your query
2. ```local|hdfs```: ```local``` store the result in a local folder, ```hdfs``` store the result in HDFS file system
3. ```limit```: k-limit value to optmize your search (```0.0``` = all the documents, ```1.0``` = exact documents).



# The Hammer Repository

Run MondoDb shell, for example
```
mongo
```

From MondoDB shell, to create the collections:

```
use hammer;
db.createCollection('index');
db.createCollection('dataset');
db.createCollection('datasource');
```

`index` is the collection for the inverted-index, `dataset` is the catalog and `datasource` is the list of the open data portals.

Insert into `datasource`, the list of source portals, for example (Regione Lombardia, Trentino, Comune di Firenze, Comune di Matera, Regione Lazio, Comune di Bari):
```
db.datasource.insert(
[{ "name" : "OPEN DATA TRENTINO - ITALIA", "url" : "http://dati.trentino.it/api/action", "type" : "org.hammer.santamaria.input.CKANSourceRecordReader" },
{ "name" : "OPEN DATA TOSCANA - ITALIA", "url" : "http://dati.toscana.it/api/action", "type" : "org.hammer.santamaria.input.CKANSourceRecordReader" },
{ "name" : "OPEN DATA COMUNE DI FIRENZE - ITALIA", "url" : "http://annuario.comune.fi.it/api/action", "type" : "org.hammer.santamaria.input.CKANSourceRecordReader" },
{ "name" : "OPEN DATA COMUNE DI MATERA - ITALIA", "url" : "http://dati.comune.matera.it/api/action", "type" : "org.hammer.santamaria.input.CKANSourceRecordReader" },
{ "name" : "REGIONE LOMBARDIA", "url" : "https://www.dati.lombardia.it/resource/425r-pyq4.json", "type" : "org.hammer.santamaria.input.SocrataSourceRecordReader" },
{ "name" : "INPS OPEN DATA", "url" : "https://serviziweb2.inps.it/odapi/catalog", "type" : "org.hammer.santamaria.input.INPSSourceRecordReader" },
{ "name" : "OPEN DATA BARI - ITALIA", "url" : "http://opendata.comune.bari.it/api/action", "type" : "org.hammer.santamaria.input.CKANSourceRecordReader" },
{ "name" : "OPEN DATA LAZIO - ITALIA", "url" : "http://dati.lazio.it/catalog/api/action", "type" : "org.hammer.santamaria.input.CKANSourceRecordReader" }]
);
```

# License

Hammer Project is released under the MIT license.

Copyright (c) Mauro Pelucchi www.mauropelucchi.com (mailto: mauro.pelucchi@gmail.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.