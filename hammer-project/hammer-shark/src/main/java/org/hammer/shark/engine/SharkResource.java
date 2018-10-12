package org.hammer.shark.engine;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.cc.util.QueryGraphCloner;
import org.hammer.isabella.fuzzy.JaroWinkler;
import org.hammer.isabella.query.Edge;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Keyword;
import org.hammer.isabella.query.Node;
import org.hammer.isabella.query.QueryGraph;
import org.hammer.isabella.query.ValueNode;
import org.hammer.shark.utils.Config;
import org.hammer.shark.utils.RecursiveString;
import org.hammer.shark.utils.SocrataUtils;
import org.hammer.shark.utils.SpaceUtils;
import org.hammer.shark.utils.StatUtils;
import org.hammer.isabella.query.Term;

import com.google.gson.Gson;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * Shark Resource Finder
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Shark
 *
 */
public class SharkResource {

	public SharkResource(SparkSession spark) {

	}

	public static final Log LOG = LogFactory.getLog(SharkResource.class);

	/**
	 * Comparator for Query
	 */
	Comparator<List<Term[]>> cmp_query = new Comparator<List<Term[]>>() {
		public int compare(List<Term[]> o1, List<Term[]> o2) {
			double sim1 = SpaceUtils.cos(o1);
			double sim2 = SpaceUtils.cos(o1);
			return (sim1 < sim2) ? 1 : ((sim1 > sim2) ? -1 : 0);
		}
	};
	
	public void getItems(SparkSession spark) throws Exception {
		
		
		System.out.println("START-STOP --> START SCHEMA FITTING " + (new Date()));
		Date start = new Date();
		
		final HashMap<String, Keyword> kwIndex = StatUtils.GetMyIndex();
		LOG.info("---> Calculate INPUTSPLIT FOR DATASET");
		MongoClientURI inputURI = new MongoClientURI(
				Config.getInstance().getConfig().getString("spark.mongodb.input.uri"));

		List<DataSetSplit> splits = new ArrayList<>();
		LOG.debug("---> Shark calculating splits for " + inputURI);
		float thSim = Float.parseFloat(spark.sparkContext().conf().get("thSim"));
		// create my query graph object
		// System.out.println(query);
		Isabella parser = new Isabella(new StringReader(spark.sparkContext().conf().get("query-string")));
		String keywords = "";
		Map<String, List<Term>> similarity = new HashMap<String, List<Term>>();
		String wnHome = Config.getInstance().getConfig().getString("wnHome");

		QueryGraph q;
		try {
			q = parser.queryGraph();
			q.setIndex(kwIndex);
			q.setWnHome(wnHome);
		} catch (ParseException e) {
			throw new Exception(e.getMessage());
		}
		for (IsabellaError err : parser.getErrors().values()) {
			LOG.error(err.toString());
		}

		if (spark.sparkContext().conf().get("query-mode").equals("labels")) {
			q.calculateMyLabels();
			spark.sparkContext().conf().set("keywords", q.getMyLabels());
			keywords = q.getMyLabels();
		} else {
			q.labelSelection();
			spark.sparkContext().conf().set("keywords", q.getKeyWords());
			keywords = q.getKeyWords();

			StringTokenizer st = new StringTokenizer(keywords, ";");
			while (st.hasMoreElements()) {
				String key = st.nextToken().trim().toLowerCase();

				ArrayList<String> tempList = new ArrayList<String>();
				for (String s : kwIndex.keySet()) {
					double sim = JaroWinkler.Apply(key, s.toLowerCase());
					// set the degree threshold to 90%
					if (sim > thSim) {
						tempList.add(s.toLowerCase());
					}
				}

				List<Term> myval = new ArrayList<Term>();
				for (String s : kwIndex.keySet()) {
					double sim = JaroWinkler.Apply(key, s.toLowerCase());
					// set the degree threshold to 90%
					if (sim > thSim) {
						Term point = new Term();
						point.setTerm(s.toLowerCase());
						point.setWeigth((double) sim);
						myval.add(point);
					}
				}
				
				

				similarity.put(key, myval);
			}
		}

		LOG.info("------------------------------------------------------");
		LOG.info("---- Create all the combination per FUZZY SEARCH -----");
		// recursive call
		List<Term[]> optionsList = new ArrayList<Term[]>();
		List<List<Term[]>> beforePrunning = new ArrayList<List<Term[]>>();
		List<List<Term[]>> cases = new ArrayList<List<Term[]>>();
		
		// calculate all the combination
		float cosSim = 	Float.parseFloat(spark.sparkContext().conf().get("cosSim"));

		RecursiveString.Recurse(optionsList, similarity, 0, beforePrunning, cosSim);
		LOG.info("--- FUZZY SEARCH QUERY --> " + cases.size());
		int thQuery = Integer.parseInt(spark.sparkContext().conf().get("thQuery"));

		// pruning this list!!!
		beforePrunning.sort(cmp_query);
		if (cases.size() > thQuery) {
			LOG.info("PRUNE THE LIST...");
			cases = beforePrunning.subList(0, thQuery);
		} else {
			cases = beforePrunning;
		}

		// qList is the list of all query for fuzzy search
		// the key of the list is a string corresponds to the keywords
		// so we remove the duplicate query!
		// also we have the keywords to operate the fuzzy search the the funcion
		// getList
		HashMap<String, QueryGraph> qList = new HashMap<String, QueryGraph>();
		// first we add the original query
		qList.put(keywords, q);

		for (int i = 0; i < cases.size(); i++) {
			LOG.debug("----> Query case " + (i + 1) + ": ");
			String keywordsCase = "";
			for (Term[] k : cases.get(i)) {
				LOG.debug(k[0].getTerm() + "-" + k[1].getTerm() + ",");
				keywordsCase += ";" + k[1].getTerm();
			}

			try {
				QueryGraph temp = QueryGraphCloner.deepCopyTerm(spark.sparkContext().conf().get("query-string"),
						cases.get(i), kwIndex);
				qList.put(keywordsCase, temp);

			} catch (Exception e) {
				LOG.error(e.getMessage());
				LOG.debug(e);
			}
		}

		LOG.info("------------------------------------------------------");
		LOG.info("------------------------------------------------------");
		LOG.info("--Total fuzzy search query " + qList.size());
		LOG.info("---- End combination for FUZZY SEARCH ----------------");
		System.out.println("START-STOP --> STOP SCHEMA FITTING " + (new Date()));
		long seconds = ((new Date()).getTime() - start.getTime());
		System.out.println("START-STOP --> TIME SCHEMA FITTING (ms)" + seconds);
		start = new Date();

		
		System.out.println("START-STOP --> START Instance Filtering " + (new Date()));

		
		// create the table for the result
		// and clean
		BSONObject statObj = new BasicBSONObject();
		statObj.put("type", "clean");
		StatUtils.SaveStat(statObj, spark.sparkContext().conf().get("list-result"),
				spark.sparkContext().conf().get("stat-result"));

		// save stat
		statObj = new BasicBSONObject();
		statObj.put("type", "stat");
		statObj.put("record-total", 0);
		statObj.put("record-selected", 0);
		statObj.put("resource-count", 0);
		statObj.put("size", 0);
		statObj.put("total-query", 0);
		statObj.put("fuzzy-query", qList.size());

		StatUtils.SaveStat(statObj, spark.sparkContext().conf().get("list-result"),
				spark.sparkContext().conf().get("stat-result"));

		// esecute the getSetList function for every fuzzy query
		// the function return the list of the resources that match with the
		// query
		// we store the data into a dataset map
		// for eliminate the duplicate resource
		Map<String, Document> dataSet = new HashMap<String, Document>();
		MongoClient mongo = null;
		MongoDatabase db = null;
		try {
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());
			// connection with dataset and index collection of mongodb
			MongoCollection<Document> dataset = db.getCollection(spark.sparkContext().conf().get("dataset-table") + "");
			MongoCollection<Document> index = db
					.getCollection(Config.getInstance().getConfig().getString("index-table") + "");

			qList.keySet().parallelStream().forEach(key -> {
				List<Document> temp = getSetList(spark, qList.get(key), key, dataset, index);

				temp.parallelStream().forEach(t -> {
					String documentKey = t.getString("_id");
					dataSet.put(documentKey, t);
				});
			});
		} catch (Exception ex) {
			LOG.error(ex.getMessage());
			LOG.debug(ex);
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}

		LOG.info("!!!!! FUZZY SEARCH has found " + dataSet.size() + " RESOURCES !!!!!");
		statObj = new BasicBSONObject();
		statObj.put("type", "stat");
		statObj.put("record-total", 0);
		statObj.put("record-selected", 0);
		statObj.put("resource-count", dataSet.size());
		statObj.put("size", 0);
		statObj.put("fuzzy-query", 0);
		statObj.put("total-query", 0);
		StatUtils.SaveStat(statObj, spark.sparkContext().conf().get("list-result"),
				spark.sparkContext().conf().get("stat-result"));
		//
		//
		//
		// pass the document to map function
		//
		//
		dataSet.values().parallelStream().forEach(doc -> {
			String key = doc.getString("_id");
			LOG.debug("---> found " + key + " - " + doc.getString("title"));
			DataSetSplit dsSplit = new DataSetSplit();
			if (spark.sparkContext().conf().get("search-mode").equals("download")) {
				dsSplit.setName(key);
				if (doc.containsKey("url") && !doc.containsKey("remove")) {
					dsSplit.setUrl(doc.getString("url"));
					dsSplit.setType(doc.getString("dataset-type"));
					dsSplit.setDataSetType(doc.getString("datainput_type"));
					dsSplit.setDatasource(doc.getString("id"));
					splits.add(dsSplit);
				}
				/*
				 * if (doc.containsKey("resources")) { ArrayList<Document> resources =
				 * (ArrayList<Document>) doc.get("resources"); for (Document resource :
				 * resources) { String rKey = key + "_" + resource.getString("id");
				 * dsSplit.setName(rKey); dsSplit.setUrl(resource.getString("url"));
				 * dsSplit.setType(doc.getString("dataset-type"));
				 * dsSplit.setDataSetType(doc.getString("datainput_type"));
				 * dsSplit.setDatasource(doc.getString("id")); splits.add(dsSplit);
				 * 
				 * } }
				 */
			} else {
				dsSplit.setName(key);
				if (doc.containsKey("url") && !doc.containsKey("remove")) {
					dsSplit.setUrl(doc.getString("url"));
					dsSplit.setType(doc.getString("dataset-type"));
					dsSplit.setDataSetType(doc.getString("datainput_type"));
					dsSplit.setDatasource(doc.getString("id"));
					dsSplit.setAction(doc.getString("id"));
					dsSplit.setDataset(doc.getString("name"));
					splits.add(dsSplit);
				}
			}
		});

		// save to splits temp table

		try {
			MongoClientURI outputURI = new MongoClientURI(
					Config.getInstance().getConfig().getString("spark.mongodb.output.uri"));
			mongo = new MongoClient(outputURI);
			db = mongo.getDatabase(outputURI.getDatabase());
			System.out.println("SHARK QUERY Create temp table split-table");
			if (db.getCollection("split_table") != null) {
				db.getCollection("split_table").drop();
			}
			db.createCollection("split_table");
			MongoCollection<Document> collection = db.getCollection("split_table");

			Gson gson = new Gson();

			for (DataSetSplit bo : splits) {

				try {

					// SAVE LIST OF RESOURCE TO MONGO DB
					BasicDBObject searchQuery = new BasicDBObject().append("_id", bo.getDataset());
					long c = collection.count(searchQuery);
					if (c == 0) {
						Document newObj = Document.parse(gson.toJson(bo));
						newObj.append("_id", bo.getName());
						collection.insertOne(newObj);
					}

				} catch (Exception e) {
					LOG.debug(e);
					LOG.error("SHARK QUERY: Error writing to temporary file", e);
					throw new IOException(e);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}

		// get list results and return record

		try {
			MongoClientURI outputURI = new MongoClientURI(
					Config.getInstance().getConfig().getString("spark.mongodb.output.uri"));
			mongo = new MongoClient(outputURI);
			db = mongo.getDatabase(outputURI.getDatabase());
			System.out.println("SHARK QUERY Create temp table record-temp-table");
			if (db.getCollection("record_temp_table") != null) {
				db.getCollection("record_temp_table").drop();
			}
			db.createCollection("record_temp_table");
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}
		Dataset<DataSetSplit> list = null;

		try {
			Map<String, String> readOverrides = new HashMap<String, String>();
			readOverrides.put("database", "hammer");
			readOverrides.put("collection", "split_table");
			JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
			ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

			list = MongoSpark.load(jsc, readConfig).toDF().dropDuplicates().map(x -> {
				DataSetSplit ds = new DataSetSplit();
				ds.setAction(x.getString(x.fieldIndex("action")));
				ds.setDataset(x.getString(x.fieldIndex("name")));
				if(x.schema().contains("dataSetType")) {
					ds.setDataSetType(x.getString(x.fieldIndex("dataSetType")));
				}
				ds.setDatasource(x.getString(x.fieldIndex("datasource")));
				ds.setName(x.getString(x.fieldIndex("name")));
				ds.setType(x.getString(x.fieldIndex("type")));
				ds.setUrl(x.getString(x.fieldIndex("url")));

				return ds;
			}, org.apache.spark.sql.Encoders.bean(DataSetSplit.class));

		} catch (Exception e) {
			LOG.error("Error during get tags", e);
			throw new Exception(e);
		}

		// download the data
		list.foreach(d -> {
			RecordReader.search(d);
		});

		// read download data and apply mapper

		Dataset<Tuple2<String, String>> tempRecord = null;

		try {
			Map<String, String> readOverrides = new HashMap<String, String>();
			readOverrides.put("database", "hammer");
			readOverrides.put("collection", "record_temp_table");
			JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
			ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

			tempRecord = MongoSpark.load(jsc, readConfig).toDF().flatMap(x -> {
				return mapper(x).iterator();
			}, org.apache.spark.sql.Encoders.tuple(org.apache.spark.sql.Encoders.STRING(),
					org.apache.spark.sql.Encoders.STRING()));

		} catch (Exception e) {
			LOG.error("Error during get tags", e);
			throw new Exception(e);
		}

		List<String> toSave = tempRecord.filter(x -> {
			return filter(x, q, thSim);
		}).map(x -> {
			return x._2;
		}, Encoders.STRING()).collectAsList();

		try {
			MongoClientURI outputURI = new MongoClientURI(
					Config.getInstance().getConfig().getString("spark.mongodb.output.uri"));
			mongo = new MongoClient(outputURI);
			final MongoDatabase db1 = mongo.getDatabase(outputURI.getDatabase());
			System.out.println("SHARK QUERY Create temp table " + spark.sparkContext().conf().get("query-table"));
			if (db1.getCollection(spark.sparkContext().conf().get("query-table")) != null) {
				db1.getCollection(spark.sparkContext().conf().get("query-table")).drop();
			}
			db1.createCollection(spark.sparkContext().conf().get("query-table"));
			
			toSave.parallelStream().forEach(bo -> {
				LOG.info(bo);
				Document obj = Document.parse(bo);
				try {
					if (ApplyWhereCondition(obj, SparkSession.builder().appName("SHARK").getOrCreate(), q, thSim)) {
						db1.getCollection(spark.sparkContext().conf().get("query-table")).insertOne(obj);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			});

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}
		
		System.out.println("START-STOP --> STOP Instance Filtering " + (new Date()));
		seconds = ((new Date()).getTime() - start.getTime());
		System.out.println("START-STOP --> TIME Instance Filtering (ms) " + seconds);

	}

	/**
	 * Contacts the db and builds a map of each set by keyword
	 * 
	 * 
	 * @return
	 */
	protected ArrayList<Document> getSetList(SparkSession spark, QueryGraph q, String keywords,
			MongoCollection<Document> dataset, MongoCollection<Document> index) {

		final ArrayList<Document> returnList = new ArrayList<Document>();
		float thKrm = Float.parseFloat(spark.sparkContext().conf().get("thKrm"));
		float thRm = Float.parseFloat(spark.sparkContext().conf().get("thRm"));

		try {

			// now search the keywords or the labels on the index (with or)
			StringTokenizer st = new StringTokenizer(keywords, ";");

			BasicDBList or = new BasicDBList();
			while (st.hasMoreElements()) {
				String word = st.nextToken().trim().toLowerCase();
				if (word.trim().length() > 2) {
					/*
					 * ArrayList<String> synonyms =
					 * ThesaurusUtils.Get(getConfiguration().get("thesaurus.url" ), word,
					 * getConfiguration().get("thesaurus.lang"),
					 * getConfiguration().get("thesaurus.key"), "json"); synonyms.add(word); for
					 * (String synonym : synonyms) { BasicDBObject temp = new
					 * BasicDBObject("keyword", new BasicDBObject("$regex", synonym)); or.add(temp);
					 * }
					 */

					// BasicDBObject temp = new BasicDBObject("keyword", new
					// BasicDBObject("$regex", word));
					BasicDBObject temp = new BasicDBObject("keyword", word);
					or.add(temp);
				}

			}

			// search the keywords on my index
			BasicDBObject searchQuery = new BasicDBObject("$or", or);
			LOG.debug("Shark gets data set from database..." + searchQuery.toString());

			FindIterable<Document> indexS = index.find(searchQuery);

			//
			// keyword find on my index
			// the structure of kwFinded
			// key = the keyword
			// value = the list of the documents associate with key
			//
			final HashMap<String, ArrayList<String>> kwFinded = new HashMap<String, ArrayList<String>>();
			indexS.forEach(new Block<Document>() {

				public void apply(final Document document) {
					@SuppressWarnings("unchecked")
					ArrayList<Document> docList = (ArrayList<Document>) document.get("documents");
					ArrayList<String> idList = new ArrayList<String>();
					for (Document doc : docList) {
						idList.add(doc.getString("document"));
					}
					if (docList != null) {
						kwFinded.put(document.getString("keyword"), idList);
					}
				}
			});
			if (kwFinded.size() == 0) {
				throw new Exception("!!!!! ERROR NOTHING FOUND !!!!");
			} else {
				LOG.info(" Found keyword with resources --> " + kwFinded.size());
			}

			// now we find the relevant resources and calculate krm
			// krm = [0,1]
			// krm = number keyword match / total number of keyword
			//
			// the rSet contains the resources
			// the hast map krmMap contains the value of krm for each resources
			BasicDBList rSet = new BasicDBList();
			HashMap<String, Float> krmMap = new HashMap<String, Float>();

			LOG.debug("Get resources and calc krm....");
			for (ArrayList<String> listId : kwFinded.values()) {
				for (String key : listId) {
					float found = 0;
					for (ArrayList<String> lista : kwFinded.values()) {
						if (lista.contains(key)) {
							found++;
						}
					}
					float krm = ((float) found / (float) kwFinded.size());
					//
					// if krm >= th ok!!!
					//
					if (krm >= thKrm) {
						BasicDBObject temp = new BasicDBObject("_id", key);
						if (!krmMap.containsKey(key)) {
							rSet.add(temp);
							krmMap.put(key, krm);
						}
					}
				}
			}

			LOG.info(" ----------------------------------------------> ");
			if (rSet.size() == 0) {
				throw new Exception("!!!!! ERROR NOTHING RESOURCE FOUND !!!!");
			} else {
				LOG.info(" --> fount relevant resources  " + rSet.size());
			}

			// now we calc sdfMap where the map key is the document key and the
			// value is the valure of sdf
			HashMap<String, Float> sdfMap = new HashMap<String, Float>();
			// calc the total weitgth
			final HashMap<String, Float> wWhere = q.getwWhere();
			final Float w = q.getWeightWhere();

			BasicDBObject searchRR = new BasicDBObject("$or", rSet);
			System.out.println("Shark gets relevant resources..." + searchRR.toString());
			FindIterable<Document> iterable = dataset.find(searchRR);
			iterable.forEach(new Block<Document>() {

				@SuppressWarnings("unchecked")
				public void apply(final Document document) {
					float sdf = 0.0f;
					ArrayList<String> meta = new ArrayList<String>();
					if (document.keySet().contains("meta")) {
						meta = (ArrayList<String>) document.get("meta");
					}
					for (String k : meta) {
						sdf += (wWhere.containsKey(k)) ? wWhere.get(k) : 0.0f;
					}
					sdf = (float) sdf / (float) w;
					sdfMap.put(document.getString("_id"), sdf);

				}
			});

			// now we have sdf and krm for each documents and can calculate rm
			// alfa = 0.6
			// r.rm= (1-alfa) * r.krm) + (alfa * r.sfd)
			//
			// th in this case is set to thRm

			// the rmMap contain the rm-value for each document_id
			//
			HashMap<String, Float> rmMap = new HashMap<String, Float>();
			//
			// the idSet contains the list of resource that are ok!
			//
			BasicDBList idSet = new BasicDBList();
			//
			for (String documentKey : krmMap.keySet()) {
				float krm = krmMap.get(documentKey);
				float sdf = (sdfMap.containsKey(documentKey)) ? sdfMap.get(documentKey) : 0.0f;
				float rm = ((1.0f - 0.6f) * krm) + (0.6f * sdf);
				rmMap.put(documentKey, rm);
				if (rm >= thRm) {
					BasicDBObject temp = new BasicDBObject("_id", documentKey);
					idSet.add(temp);
				}
			}

			if (idSet.size() == 0) {
				throw new Exception("!!!!! ERROR NOTHING RELEVANT RESOURCES FOUND (with rm >= " + thRm + ") !!!!");
			} else {
				LOG.info("--- > FOUND RELEVANT RESOURCES FOUND (with rm >= " + thRm + ") " + idSet.size());
			}

			// search the resources
			// and if i want to search we must add the JSON constrain, else not
			searchQuery = new BasicDBObject("$or", idSet);
			searchQuery.append("dataset-type", new BasicDBObject("$regex", "JSON"));

			System.out.println("Shark gets dataset from database..." + searchQuery.toString());
			//
			// rmList is the final List of resources!!!
			final ArrayList<Document> rmList = new ArrayList<Document>();
			iterable = dataset.find(searchQuery);
			iterable.forEach(new Block<Document>() {
				public void apply(final Document document) {
					rmList.add(document);

				}
			});

			// before return split check socrata case
			rmList.parallelStream().forEach(doc -> {

				try {
					// if socrata split in set by 5000 record
					if (doc.containsKey("datainput_type") && doc.get("datainput_type")
							.equals("org.hammer.santamaria.mapper.dataset.SocrataDataSetInput")) {

						String socrataQuery = SocrataUtils.CreateWhereCondition(spark, doc.getString("_id"));

						if (socrataQuery.length() > 0 && doc.getString("dataset-type").equals("JSON")) {
							long count = SocrataUtils.CountPackageList(spark, doc.getString("url"),
									doc.getString("_id"));
							int offset = 0;

							if (count > 0) {
								while (offset < count) {
									Document tempDoc = new Document(doc);
									String tempUrl = SocrataUtils.GetUrl(spark, doc.getString("_id"),
											doc.getString("url"), offset, 1000, socrataQuery);
									tempDoc.replace("url", tempUrl);
									tempDoc.replace("_id", doc.get("_id") + "_" + offset);
									returnList.add(tempDoc);
									// save stat
									StatUtils.UpdateResultList(tempDoc, spark.sparkContext().conf().get("list-result"));
									offset = offset + 1000;
								}
							}
						}

					} else {
						// save stat
						StatUtils.UpdateResultList(doc, spark.sparkContext().conf().get("list-result"));
						returnList.add(doc);
					}
				} catch (Exception e) {
					LOG.error(e);
				}
			});

		} catch (Exception ex) {
			LOG.error(ex.getMessage());
			LOG.debug(ex);
		}
		LOG.info("Shark find " + returnList.size());

		return returnList;
	}

	public static List<Tuple2<String, String>> mapper(Row ds) {

		List<Tuple2<String, String>> lists = new ArrayList<>();

		// save stat
		BSONObject statObj = new BasicBSONObject();
		statObj.put("type", "resource");
		statObj.put("name", (String) ds.getString(ds.fieldIndex("dataset")));
		statObj.put("count", 1);
		StatUtils.SaveStat(statObj,
				SparkSession.builder().appName("SHARK").getOrCreate().sparkContext().conf().get("list-result"),
				SparkSession.builder().appName("SHARK").getOrCreate().sparkContext().conf().get("stat-result"));

		Gson g = new Gson();
		List<String> a = Arrays.asList(ds.schema().fieldNames());
		String sValue = g.toJson(ds.getValuesMap(JavaConverters.asScalaIteratorConverter(a.iterator()).asScala().toSeq()));

		for (String column : ds.schema().fieldNames()) {
			Object value = ds.get(ds.fieldIndex(column));
			String key = column + "|" + value;
			lists.add(new Tuple2<String, String>(key, sValue));
		}
		return lists;
	}

	public static boolean filter(Tuple2<String, String> ds, QueryGraph q, float thSim) {
		LOG.debug("START SHARK REDUCER ");
		boolean check = false;
		//
		// key = column-value --> pKey
		// value = the record --> pValues
		// so
		// if key match the where condition we take the record
		// else we doesn't store the record

		LOG.info("---------------------------------------------------");
		StringTokenizer st = new StringTokenizer(ds._1.toString(), "|");
		String column = st.nextToken().toLowerCase();

		String value = "";
		if (st.hasMoreTokens()) {
			value = st.nextToken().toLowerCase();
		}
		long size = 0;
		long totalRecord = 0;
		long selectedRecord = 0;

		Map<String, List<String>> synset = new HashMap<String, List<String>>();

		boolean orCheck = false;
		int orCount = 0;
		for (Edge en : q.getQueryCondition()) {
			LOG.info(en.getCondition());
			LOG.info(en.getOperator());
			LOG.info("------------------------------------");
			for (Node ch : en.getChild()) {

				if ((ch instanceof ValueNode) && en.getCondition().equals("or")) {
					orCount++;

					LOG.info(en.getName().toLowerCase() + " -- " + column.toLowerCase());
					LOG.info(ch.getName().toLowerCase() + " -- " + value);

					boolean syn = CheckSynset(en.getName().toLowerCase(), column.toLowerCase(), synset);
					double sim = JaroWinkler.Apply(en.getName().toLowerCase(), column.toLowerCase());

					LOG.info("test " + sim + ">=" + thSim + " -- syn: " + syn);
					if ((sim >= thSim) || (syn)) {
						LOG.info("ok sim --> " + sim);
						LOG.info("ok syn --> " + syn);

						LOG.info("check  --> " + ch.getName().toLowerCase().compareTo(value));
						double simV = JaroWinkler.Apply(ch.getName().toLowerCase(), value.toLowerCase());
						LOG.info("check  --> " + simV);
						if (column.toLowerCase().contains("date")) {
							// it's a date
							try {
								SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
								Date dx = formatDate.parse(value.toLowerCase());
								Date sx = formatDate.parse(ch.getName().toLowerCase());

								if (en.getOperator().equals("eq") && sx.equals(dx)) {
									orCheck = true;
								} else if (en.getOperator().equals("gt")) {
									if (sx.compareTo(dx) > 0) {
										orCheck = true;
									}
								} else if (en.getOperator().equals("lt")) {
									if (sx.compareTo(dx) < 0) {
										orCheck = true;
									}
								} else if (en.getOperator().equals("ge")) {
									if (sx.compareTo(dx) >= 0) {
										orCheck = true;
									}
								} else if (en.getOperator().equals("le")) {
									if (sx.compareTo(dx) <= 0) {
										orCheck = true;
									}
								}
							} catch (Exception ex) {
								LOG.error(ex);
							}

						} else if (en.getOperator().equals("eq")
								&& (ch.getName().toLowerCase().equals(value) || (simV >= thSim))) {
							orCheck = true;
						} else if (en.getOperator().equals("gt")) {
							if (ch.getName().toLowerCase().compareTo(value) > 0) {
								orCheck = true;
							}
						} else if (en.getOperator().equals("lt")) {
							if (ch.getName().toLowerCase().compareTo(value) < 0) {
								orCheck = true;
							}
						} else if (en.getOperator().equals("ge")) {
							if (ch.getName().toLowerCase().compareTo(value) >= 0) {
								orCheck = true;
							}
						} else if (en.getOperator().equals("le")) {
							if (ch.getName().toLowerCase().compareTo(value) <= 0) {
								orCheck = true;
							}
						} else if (en.getName().equals(column)) {
							if (ch.getName().toLowerCase().compareTo(value) == 0) {
								orCheck = true;
							}
						}
					}

				}

			}
		}

		if (orCheck || (orCount == 0)) {
			check = true;
			selectedRecord++;
			size += 1;
			totalRecord++;
		}

		// save the stat
		BSONObject statObj = new BasicBSONObject();
		statObj.put("type", "stat");
		statObj.put("record-total", totalRecord);
		statObj.put("record-selected", selectedRecord);
		statObj.put("resource-count", 0);
		statObj.put("size", size);
		statObj.put("fuzzy-query", 0);
		statObj.put("total-query", 0);
		StatUtils.SaveStat(statObj,
				SparkSession.builder().appName("SHARK").getOrCreate().sparkContext().conf().get("list-result"),
				SparkSession.builder().appName("SHARK").getOrCreate().sparkContext().conf().get("stat-result"));

		return check;

	}

	/**
	 * Verify if field is in synset of column
	 * 
	 * @param column
	 *            the column
	 * @param field
	 *            the field
	 * @param conf
	 *            the configuration for access hadoop
	 * @return true or false
	 */
	private static boolean CheckSynset(String column, String field, Map<String, List<String>> synset) {

		if (synset.containsKey(column)) {
			List<String> mySynSet = synset.get(column.toLowerCase());
			return mySynSet.contains(field.toLowerCase());
		}

		boolean check = false;
		MongoClient mongo = null;
		MongoDatabase db = null;
		try {
			MongoClientURI inputURI = new MongoClientURI(
					Config.getInstance().getConfig().getString("spark.mongodb.input.uri"));
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());
			MongoCollection<Document> myIdx = db
					.getCollection(Config.getInstance().getConfig().getString("index-table") + "");
			BasicDBObject searchQuery = new BasicDBObject().append("keyword", column.toLowerCase());
			FindIterable<Document> myTerm = myIdx.find(searchQuery);
			if (myTerm.iterator().hasNext()) {
				Document obj = myTerm.iterator().next();
				if (obj != null && obj.containsKey("syn-set") && obj.get("syn-set") != null) {
					@SuppressWarnings("unchecked")
					ArrayList<Document> dbSynSet = (ArrayList<Document>) obj.get("syn-set");
					ArrayList<String> mySynSet = new ArrayList<String>();
					if (mySynSet != null) {
						for (Document o : dbSynSet) {
							mySynSet.add((o.get("term") + "").toLowerCase());
						}
					}
					synset.put(column.toLowerCase(), mySynSet);
				}
			}

			if (synset.containsKey(column)) {
				List<String> mySynSet = synset.get(column.toLowerCase());
				check = mySynSet.contains(field.toLowerCase());
			}

		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
			LOG.error(ex.getMessage());
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}

		return check;
	}

	/**
	 * Apply where condition
	 * 
	 * @param conf
	 * @param q
	 */
	private static boolean ApplyWhereCondition(Document bo, SparkSession spark, QueryGraph q, float thSim)
			throws Exception {
		//
		// check OR condition
		//
		/*
		 * 
		 * boolean orCheck = false; int orCount = 0; for (Edge en :
		 * q.getQueryCondition()) { LOG.info(en.getCondition());
		 * LOG.info(en.getOperator()); LOG.info("------------------------------------");
		 * for (Node ch : en.getChild()) {
		 * 
		 * 
		 * 
		 * if ((ch instanceof ValueNode) && en.getCondition().equals("or")) { orCount
		 * ++;
		 * 
		 * for (String column : bo.keySet()) { String value = bo.getString(column);
		 * 
		 * LOG.info(en.getName().toLowerCase() + " -- " + column.toLowerCase());
		 * LOG.info(ch.getName().toLowerCase() + " -- " + value);
		 * 
		 * boolean syn = checkSynset(en.getName().toLowerCase(), column.toLowerCase(),
		 * conf); double sim = JaroWinkler.Apply(en.getName().toLowerCase(),
		 * column.toLowerCase());
		 * 
		 * LOG.info("test " + sim + ">=" + thSim + " -- syn: " + syn); if ((sim >=
		 * thSim)||(syn)) { LOG.info("ok sim --> " + sim); LOG.info( "ok syn --> " +
		 * syn);
		 * 
		 * LOG.info("check  --> " + ch.getName().toLowerCase().compareTo(value)); double
		 * simV = JaroWinkler.Apply(ch.getName().toLowerCase(), value.toLowerCase());
		 * LOG.info("check  --> " + simV); if (en.getOperator().equals("eq") &&
		 * (ch.getName().toLowerCase().equals(value) || (simV >= thSim))) { orCheck =
		 * true; } else if (en.getOperator().equals("gt")) { if
		 * (ch.getName().toLowerCase().compareTo(value) > 0) { orCheck = true; } } else
		 * if (en.getOperator().equals("lt")) { if
		 * (ch.getName().toLowerCase().compareTo(value) < 0) { orCheck = true; } } else
		 * if (en.getOperator().equals("ge")) { if
		 * (ch.getName().toLowerCase().compareTo(value) >= 0) { orCheck = true; } } else
		 * if (en.getOperator().equals("le")) { if
		 * (ch.getName().toLowerCase().compareTo(value) <= 0) { orCheck = true; } } else
		 * if (en.getName().equals(column)) { if
		 * (ch.getName().toLowerCase().compareTo(value) == 0) { orCheck = true; } } }
		 * 
		 * } }
		 * 
		 * } }
		 * 
		 * LOG.info("---> " + orCheck);
		 * 
		 */

		//
		// check AND condition
		//

		Map<String, List<String>> synset = new HashMap<String, List<String>>();

		boolean check = true;
		int c = 0;
		List<String> andColumn = new ArrayList<String>();
		for (Edge en : q.getQueryCondition()) {
			for (Node ch : en.getChild()) {

				if ((ch instanceof ValueNode) && en.getCondition().equals("and")) {
					c++;
					// calc the best similarity column
					double maxSim = 0.0d;
					String myColumn = "";
					for (String column : bo.keySet()) {
						boolean syn = CheckSynset(en.getName().toLowerCase(), column.toLowerCase(), synset);
						double sim = JaroWinkler.Apply(en.getName().toLowerCase(), column.toLowerCase());
						if ((sim >= maxSim) || syn) {
							myColumn = column;
							maxSim = sim;
						}

					}
					andColumn.add(myColumn);
				}
			}
		}
		for (Edge en : q.getQueryCondition()) {
			LOG.info(en.getCondition());
			LOG.info(en.getOperator());
			LOG.info("------------------------------------");

			for (Node ch : en.getChild()) {

				if ((ch instanceof ValueNode) && en.getCondition().equals("and")) {
					for (String column : andColumn) {

						boolean syn = CheckSynset(en.getName().toLowerCase(), column.toLowerCase(), synset);
						double sim = JaroWinkler.Apply(en.getName().toLowerCase(), column.toLowerCase());
						String value = bo.getString(column).toLowerCase();

						LOG.info(en.getName().toLowerCase() + " -- " + column.toLowerCase());
						LOG.info(ch.getName().toLowerCase() + " -- " + value);

						LOG.info("test " + sim + ">=" + thSim + " -- syn: " + syn);
						if ((sim >= thSim) || (syn)) {
							LOG.info("ok sim --> " + sim);
							LOG.info("ok syn --> " + syn);

							// c--;
							double simV = JaroWinkler.Apply(ch.getName().toLowerCase(), value.toLowerCase());
							LOG.info("check  --> " + simV + (simV < thSim));
							if (column.toLowerCase().contains("date")) {
								// it's a date
								try {
									LOG.info("---------------> DATE CHECK ");
									SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd't'HH:mm:ss");
									Date sx = formatDate.parse(value.toLowerCase());
									Date dx = formatDate.parse(ch.getName().toLowerCase());
									LOG.info("--> DATE CHECK " + sx + en.getOperator() + dx);
									LOG.info("--> DATE CHECK " + sx.compareTo(dx));
									if (en.getOperator().equals("eq") && !sx.equals(dx)) {
										check = false;
									} else if (en.getOperator().equals("gt")) {
										if (sx.compareTo(dx) <= 0) {
											check = false;
										}
									} else if (en.getOperator().equals("lt")) {
										if (sx.compareTo(dx) >= 0) {
											check = false;
										}
									} else if (en.getOperator().equals("ge")) {
										if (sx.compareTo(dx) < 0) {
											check = false;
										}
									} else if (en.getOperator().equals("le")) {
										if (sx.compareTo(dx) > 0) {
											check = false;
										}
									}
								} catch (Exception ex) {
									LOG.error(ex);
								}

							} else if (en.getOperator().equals("eq") && !ch.getName().toLowerCase().equals(value)
									&& (simV < thSim)) {
								check = false;
							} else if (en.getOperator().equals("gt")) {
								if (ch.getName().toLowerCase().compareTo(value) <= 0) {
									check = false;
								}
							} else if (en.getOperator().equals("lt")) {
								if (ch.getName().toLowerCase().compareTo(value) >= 0) {
									check = false;
								}
							} else if (en.getOperator().equals("ge")) {
								if (ch.getName().toLowerCase().compareTo(value) < 0) {
									check = false;
								}
							} else if (en.getOperator().equals("le")) {
								if (ch.getName().toLowerCase().compareTo(value) > 0) {
									check = false;
								}
							} /*
								 * else if (en.getName().equals(column)) { if
								 * (ch.getName().toLowerCase().compareTo(value) != 0) { check = false; } }
								 */

							LOG.info("check status  --> " + check);
							LOG.info("*************************************************");
						}
					}

				}
			}
		}

		LOG.info("------------------------------------> check and condition " + c + " - " + check + " - "
				+ (c == 0 || check));

		return (c == 0 || check) /* && (orCheck || orCount == 0)) */ ;

	}
}
