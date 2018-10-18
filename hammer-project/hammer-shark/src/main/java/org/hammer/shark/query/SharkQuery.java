package org.hammer.shark.query;

import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.fuzzy.JaroWinkler;
import org.hammer.isabella.fuzzy.WordNetUtils;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Keyword;
import org.hammer.isabella.query.QueryGraph;
import org.hammer.isabella.query.Term;
import org.hammer.shark.utils.Config;
import org.hammer.shark.utils.RecursiveString;
import org.hammer.shark.utils.SocrataUtils;
import org.hammer.shark.utils.SpaceUtils;
import org.hammer.shark.utils.StatUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.splitter.SplitFailedException;

import scala.Tuple2;

/**
 * SHARK Query
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - SHARK
 *
 */
public class SharkQuery implements Serializable {

	public SharkQuery(SparkSession spark) {

	}

	public static final Log LOG = LogFactory.getLog(SharkQuery.class);

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

	/**
	 * Comparator for Term
	 */
	Comparator<Term> cmp = new Comparator<Term>() {
		public int compare(Term o1, Term o2) {
			return (o1.getWeigth() < o2.getWeigth()) ? 1 : ((o1.getWeigth() > o2.getWeigth()) ? -1 : 0);
		}
	};

	public void calculateResources(SparkSession spark) throws Exception {
		final HashMap<String, Keyword> kwIndex = StatUtils.GetMyIndex();

		Date start = (new Date());
		System.out.println("START-STOP --> START TERM EXTRACTION " + (new Date()));
		LOG.info("---> Calculate INPUTSPLIT FOR QUERY");
		MongoClientURI inputURI = new MongoClientURI(
				Config.getInstance().getConfig().getString("spark.mongodb.input.uri"));

		LOG.debug("---> SHARK Query calculating splits for " + inputURI);
		float thSim = Float.parseFloat(spark.sparkContext().conf().get("thSim"));
		float thKrm = Float.parseFloat(spark.sparkContext().conf().get("thKrm"));
		float thRm = Float.parseFloat(spark.sparkContext().conf().get("thRm"));
		int thQuery = Integer.parseInt(spark.sparkContext().conf().get("thQuery"));
		float cosSim = 	Float.parseFloat(spark.sparkContext().conf().get("cosSim"));

		int maxSim = Integer.parseInt(spark.sparkContext().conf().get("maxSim"));
		String searchMode = spark.sparkContext().conf().get("search-mode");
		String wnHome = Config.getInstance().getConfig().getString("wnHome");
		String word2vecmodel = Config.getInstance().getConfig().getString("word2vecmodel") + "";

		Word2VecModel my_model = Word2VecModel.load(word2vecmodel);

		// create my query graph object
		// System.out.println(query);
		Isabella parser = new Isabella(new StringReader(spark.sparkContext().conf().get("query-string")));
		String keywords = "";
		Map<String, List<Term>> similarity = new HashMap<String, List<Term>>();
		QueryGraph q;
		try {
			q = parser.queryGraph();
			q.setIndex(kwIndex);
			q.setWnHome(wnHome);
		} catch (ParseException e) {
			throw new SplitFailedException(e.getMessage());
		}
		for (IsabellaError err : parser.getErrors().values()) {
			LOG.error(err.toString());
		}

		// query splitter old
		/*
		 * if (getConfiguration().get("query-mode").equals("labels")) {
		 * q.calculateMyLabels(); getConfiguration().set("keywords", q.getMyLabels());
		 * keywords = q.getMyLabels(); } else { q.labelSelection();
		 * getConfiguration().set("keywords", q.getKeyWords()); keywords =
		 * q.getKeyWords();
		 * 
		 * StringTokenizer st = new StringTokenizer(keywords, ";"); while
		 * (st.hasMoreElements()) { String key = st.nextToken().trim().toLowerCase();
		 * 
		 * List<Term> tempList = new ArrayList<Term>(); for (String s :
		 * kwIndex.keySet()) { double sim = JaroWinkler.Apply(key, s.toLowerCase()); //
		 * set the degree threshold to custom value if (sim > thSim) { Term point = new
		 * Term(); point.setTerm(s.toLowerCase()); point.setWeigth(sim);
		 * tempList.add(point);
		 * 
		 * } }
		 * 
		 * // add synset by word net Map<String, Double> mySynSet =
		 * WordNetUtils.MySynset(wnHome, key.toLowerCase());
		 * 
		 * 
		 * for (String s : mySynSet.keySet()) { if (kwIndex.containsKey(s)) { Term point
		 * = new Term(); point.setTerm(s.toLowerCase());
		 * point.setWeigth(mySynSet.get(s)); tempList.add(point); } }
		 * 
		 * // cut the queue to maxsim tempList.sort(cmp); if(tempList.size() > maxSim) {
		 * tempList = tempList.subList(0, maxSim); }
		 * 
		 * similarity.put(key, tempList); } }
		 */

		q.calculateMyLabels();
		spark.sparkContext().conf().set("mylabels", q.getMyLabels());
		keywords = q.getMyLabels();

		StringTokenizer st = new StringTokenizer(keywords, ";");
		while (st.hasMoreElements()) {
			String key = st.nextToken().trim().toLowerCase();

			// add synset by jaro winkler
			Map<String, Term> tempList = new HashMap<String, Term>();

			Term point = new Term();
			point.setTerm(key.toLowerCase());
			point.setWeigth(1.0d);
			tempList.put(key.toLowerCase(), point);

			for (String s : kwIndex.keySet()) {
				double sim = JaroWinkler.Apply(key, s.toLowerCase());
				// set the degree threshold to custom value
				if (sim > thSim) {
					point = new Term();
					point.setTerm(s.toLowerCase());
					point.setWeigth(sim);
					tempList.put(s.toLowerCase(), point);

				}
			}

			// add synset by word net
			Map<String, Double> mySynSet = WordNetUtils.MySynset(wnHome, key.toLowerCase());

			for (String synset : mySynSet.keySet()) {
				// add most similar term by our index
				double max_sim = 0;
				String selected = "";
				for (String s : kwIndex.keySet()) {
					double sim = JaroWinkler.Apply(synset, s.toLowerCase());
					// set the degree threshold to custom value
					if (sim > max_sim) {
						max_sim = sim;
						selected = synset;
					}
					if (max_sim == 1) {
						break;
					}
				}

				if (!tempList.containsKey(selected.toLowerCase())) {
					point = new Term();
					point.setTerm(selected.toLowerCase());
					point.setWeigth(max_sim);
					tempList.put(selected.toLowerCase(), point);
				}
			}

			// add synset by word2vec model
			try {
				Tuple2<String, Object>[] my_array = my_model.findSynonymsArray(key.toLowerCase(), 5);

				for (Tuple2<String, Object> synonym : my_array) {
					if (!tempList.containsKey(synonym._1.toLowerCase())) {
						point = new Term();
						point.setTerm(synonym._1.toLowerCase());
						point.setWeigth((double) synonym._2);
						tempList.put(synonym._1.toLowerCase(), point);
					}
				}
			} catch (Exception ex) {
				//
			}

			// cut the queue to maxsim
			List<Term> myval = new ArrayList<Term>(tempList.values());
			myval.sort(cmp);
			if (myval.size() > maxSim) {
				myval = myval.subList(0, maxSim);
			}

			similarity.put(key, myval);
		}

		// print all combination found for testbed
		for (String k : similarity.keySet()) {
			List<Term> l = similarity.get(k);
			String temp = "";
			for (Term t : l) {
				temp += t.getTerm() + " ";
			}
			System.out.println("--> [" + k + "] : {" + temp.trim() + "}");
		}
		System.out.println("START-STOP --> STOP TERM EXTRACTION " + (new Date()));
		long seconds = ((new Date()).getTime() - start.getTime());
		System.out.println("START-STOP --> TIME TERM EXTRACTION (ms) " + seconds);
		start = new Date();
		
		System.out.println("START-STOP --> START Neighbour Queries " + (new Date()));

		
		LOG.info("------------------------------------------------------");
		LOG.info("---- Create all the combination per FUZZY SEARCH -----");
		// recursive call
		List<Term[]> optionsList = new ArrayList<Term[]>();
		List<List<Term[]>> beforePrunning = new ArrayList<List<Term[]>>();
		List<List<Term[]>> cases = new ArrayList<List<Term[]>>();

		// calculate all the combination
		LOG.info("---> SHARK Query limit " + thQuery);
		LOG.info("---> SHARK Query similarity " + cosSim);
		LOG.info("--- FUZZY SEARCH QUERY --> " + beforePrunning.size());

		RecursiveString.Recurse(optionsList, similarity, 0, beforePrunning, cosSim);

		int fuzzyQueryBefore = beforePrunning.size();

		// check the generate query with the main query and remove the major
		// distance query
		// prunnning with a fix number of neithbour query
		// for (List<Term[]> testq : beforePrunning) {
		// double sim = SpaceUtils.cos(testq);
		// if (sim >= thQuery) {
		// cases.add(testq);
		// }
		// }

		beforePrunning.sort(cmp_query);
		if (beforePrunning.size() > thQuery) {
			LOG.info("PRUNE THE LIST...");
			cases = beforePrunning.subList(0, thQuery);
		} else {
			cases = beforePrunning;
		}

		//

		System.out.println("#############################################################################");
		LOG.info("--- FUZZY SEARCH QUERY AFTER PRUNNING --> " + cases.size());
		System.out.println("START-STOP --> STOP Neighbour Queries " + (new Date()));
		seconds = ((new Date()).getTime() - start.getTime());
		System.out.println("START-STOP --> TIME Neighbour Queries (ms) " + seconds);
		start = new Date();

		System.out.println("START-STOP --> START Keyword Selection " + (new Date()));

		// print selected query
		for (List<Term[]> testq : cases) {
			System.out.println("-------------------------------------------");
			for (Term[] l : testq) {
				String temp = "";
				for (Term t : l) {
					temp += t.getTerm() + " ";
				}
				System.out.println("--> {" + temp.trim() + "}");
			}
		}

		System.out.println("");
		System.out.println("");
		System.out.println("#############################################################################");

		// qSplit is the list of all query for fuzzy search
		List<QuerySplit> qSplit = new ArrayList<QuerySplit>();
		// first we add the original query
		QuerySplit qOne = new QuerySplit();
		qOne.setKeywords(keywords);
		qOne.setQueryString(spark.sparkContext().conf().get("query-string"));
		qSplit.add(qOne);

		for (int i = 0; i < cases.size(); i++) {
			LOG.debug("----> Query case " + (i + 1) + ": ");
			String keywordsCase = "";
			for (Term[] k : cases.get(i)) {
				LOG.debug(k[0] + "-" + k[1].getTerm() + ",");
				keywordsCase += ";" + k[1].getTerm();
			}
			String newQuery = getQuery(spark.sparkContext().conf().get("query-string"), cases.get(i));
			QuerySplit newQ = new QuerySplit();
			newQ.setKeywords(keywordsCase);
			newQ.setQueryString(newQuery);
			qSplit.add(newQ);
		}

		LOG.info("------------------------------------------------------");
		LOG.info("------------------------------------------------------");
		LOG.info("--   Total fuzzy search query " + qSplit.size());
		LOG.info("---- End combination for FUZZY SEARCH ----------------");

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
		statObj.put("fuzzy-query", qSplit.size());
		statObj.put("total-query", fuzzyQueryBefore);

		StatUtils.SaveStat(statObj, spark.sparkContext().conf().get("list-result"),
				spark.sparkContext().conf().get("stat-result"));

		// clean mapper temp table
		// get list results and return record
		MongoClient mongo = null;
		MongoDatabase db = null;
				try {
					MongoClientURI outputURI = new MongoClientURI(
							Config.getInstance().getConfig().getString("spark.mongodb.output.uri"));
					mongo = new MongoClient(outputURI);
					db = mongo.getDatabase(outputURI.getDatabase());
					System.out.println("SHARK QUERY Create temp table " + spark.sparkContext().conf().get("resource-table"));
					if (db.getCollection(spark.sparkContext().conf().get("resource-table")) != null) {
						db.getCollection(spark.sparkContext().conf().get("resource-table")).drop();
					}
					db.createCollection(spark.sparkContext().conf().get("resource-table"));
				} catch (Exception ex) {
					ex.printStackTrace();
				} finally {
					if (mongo != null) {
						mongo.close();
					}
				}
		
		// start mapper
		
		String resTable = spark.sparkContext().conf().get("resource-table");
		Dataset<QuerySplit> ds = spark.createDataset(qSplit, Encoders.bean(QuerySplit.class));
		ds.foreach(x -> {
			mapper(x, wnHome, kwIndex, searchMode, thKrm, thRm, resTable);
		});
		
		
		
		/*List<Document> myList = qSplit.parallelStream().map(x -> {
			return mapper(x, wnHome, kwIndex, searchMode, thKrm, thRm);
		}).collect(Collectors.toList());*/
		
		
		System.out.println("START-STOP --> STOP Keyword Selection " + (new Date()));
		seconds = ((new Date()).getTime() - start.getTime());
		System.out.println("START-STOP --> TIME Keyword Selection (ms) " + seconds);
		start = new Date();

		System.out.println("START-STOP --> START VSM Data Set Retrieval " + (new Date()));

		
		/* MongoClient mongo = null;
		MongoDatabase db = null;
		try {
			MongoClientURI outputURI = new MongoClientURI(
					Config.getInstance().getConfig().getString("spark.mongodb.output.uri"));
			mongo = new MongoClient(outputURI);
			db = mongo.getDatabase(outputURI.getDatabase());
			System.out.println("SHARK QUERY Create temp table " + spark.sparkContext().conf().get("resource-table"));
			if (db.getCollection(spark.sparkContext().conf().get("resource-table")) != null) {
				db.getCollection(spark.sparkContext().conf().get("resource-table")).drop();
			}
			db.createCollection(spark.sparkContext().conf().get("resource-table"));
			MongoCollection<Document> collection = db.getCollection(spark.sparkContext().conf().get("resource-table"));

			int inserted = 0;
			for(Document bo: myList) {
				if(bo!= null) {
					try {

						// SAVE LIST OF RESOURCE TO MONGO DB
						BasicDBObject searchQuery = new BasicDBObject().append("_id", bo.get("_id"));
						long c = collection.count(searchQuery);
						if (c == 0) {
							collection.insertOne(bo);
							inserted++;
						}

					} catch (Exception e) {
						LOG.debug(e);
						LOG.error("SHARK QUERY: Error reading from temporary file", e);
						throw new IOException(e);
					}
				}
			}
			
	*/
		try {
			MongoClientURI outputURI = new MongoClientURI(
					Config.getInstance().getConfig().getString("spark.mongodb.output.uri"));
			mongo = new MongoClient(outputURI);
			db = mongo.getDatabase(outputURI.getDatabase());
			
			long inserted = db.getCollection(spark.sparkContext().conf().get("resource-table")).count();

			
			LOG.info("SHARK QUERY INSERT - DATA SET : " + inserted);

			// save the stat
			statObj = new BasicBSONObject();
			statObj.put("type", "stat");
			statObj.put("record-total", 0);
			statObj.put("record-selected", 0);
			statObj.put("resource-count", inserted);
			statObj.put("size", 0);
			statObj.put("fuzzy-query", 0);
			statObj.put("total-query", 0);
			StatUtils.SaveStat(statObj, spark.sparkContext().conf().get("list-result"),
					spark.sparkContext().conf().get("stat-result"));
			
			
			System.out.println("START-STOP --> STOP VSM Data Set Retrieval " + (new Date()));
			seconds = ((new Date()).getTime() - start.getTime());
			System.out.println("START-STOP --> TIME VSM Data Set Retrieval (ms) " + seconds);
			start = new Date();
			System.out.println("START-STOP --> START Instance Filtering " + (new Date()));

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}
		
				
		
		
		
		
		spark.stop();
	}

	private static void mapper(QuerySplit pValue, String wnHome, HashMap<String, Keyword> kwIndex, String searchMode,
			float thKrm, float thRm, String resourceTable) {
		LOG.info("START SHARK QUERY MAPPER " + pValue.getQueryString());

		if (pValue != null) {
			LOG.info("START SHARK MAPPER - Dataset " + pValue.getQueryString() + " --- " + pValue.hashCode());
			// get the query string from pValue
			String queryString = pValue.getQueryString();
			Isabella parser = new Isabella(new StringReader(queryString));
			QueryGraph query;

			try {
				query = parser.queryGraph();
				query.setIndex(kwIndex);
				query.setWnHome(wnHome);
			} catch (ParseException e) {
				return;
			}

			// select label
			query.labelSelection();

			for (IsabellaError err : parser.getErrors().values()) {
				LOG.error(err.toString());
			}

			if (parser.getErrors().size() == 0) {
				// search document for this query
				// execute the getSetList function for every fuzzy query
				// the function return the list of the resources that match with
				// the
				// query
				// we store the data into a dataset map
				// for eliminate the duplicate resource
				MongoClientURI inputURI = new MongoClientURI(
						Config.getInstance().getConfig().getString("spark.mongodb.input.uri"));
				Map<String, Document> dataSet = new HashMap<String, Document>();
				MongoClient mongo = null;
				MongoDatabase db = null;
				try {
					mongo = new MongoClient(inputURI);
					db = mongo.getDatabase(inputURI.getDatabase());
					// connection with dataset and index collection of mongodb
					MongoCollection<Document> dataset = db.getCollection(inputURI.getCollection());
					MongoCollection<Document> index = db
							.getCollection(Config.getInstance().getConfig().getString("index-table") + "");

					List<Document> temp = getSetList(query, query.getKeyWords(), dataset, index, thKrm, thRm);

					for (Document t : temp) {
						String documentKey = t.getString("_id");
						dataSet.put(documentKey, t);

						LOG.debug("---> found " + documentKey + " - " + t.getString("title"));
						Document resource = new Document();
						if (searchMode.equals("download")) {
							resource.put("_id", documentKey);
							if (t.containsKey("url") && !t.containsKey("remove")) {
								resource.put("_id", documentKey);
								resource.put("url", t.getString("url"));
								resource.put("dataset-type", t.getString("dataset-type"));
								resource.put("datainput_type", t.getString("datainput_type"));
								resource.put("id", t.getString("id"));

								resource.put("rm", t.get("rm"));
								resource.put("krm", t.get("krm"));
								resource.put("keywords", t.get("keywords"));
								db.getCollection(resourceTable).insertOne(resource);

								return;

							}

						} else {
							resource.put("_id", documentKey);
							if (t.containsKey("url") && !t.containsKey("remove")) {
								resource.put("_id", documentKey);
								resource.put("url", t.getString("url"));
								resource.put("dataset-type", t.getString("dataset-type"));
								resource.put("datainput_type", t.getString("datainput_type"));
								resource.put("id", t.getString("id"));
								resource.put("rm", t.get("rm"));
								resource.put("krm", t.get("krm"));
								resource.put("keywords", t.get("keywords"));

								db.getCollection(resourceTable).insertOne(resource);

								return;

							}
						}
					}

				} catch (Exception ex) {
					LOG.error(ex.getMessage());
					LOG.debug(ex);
				} finally {
					if (mongo != null) {
						mongo.close();
					}
				}

			}

		}

	}

	/**
	 * Contacts the db and builds a map of each set by keyword
	 * 
	 * 
	 * @return
	 */
	private static ArrayList<Document> getSetList(QueryGraph q, String keywords, MongoCollection<Document> dataset,
			MongoCollection<Document> index, float thKrm, float thRm) {

		final ArrayList<Document> returnList = new ArrayList<Document>();

		try {

			// now search the keywords or the labels on the index (with or)
			StringTokenizer st = new StringTokenizer(keywords, ";");

			BasicDBList or = new BasicDBList();
			while (st.hasMoreElements()) {
				String word = st.nextToken().trim().toLowerCase();
				if (word.trim().length() > 2) {
					/*
					 * ArrayList<String> synonyms = ThesaurusUtils.Get(conf.get("thesaurus.url" ),
					 * word, conf.get("thesaurus.lang"), conf.get("thesaurus.key"), "json");
					 * synonyms.add(word); for (String synonym : synonyms) { BasicDBObject temp =
					 * new BasicDBObject("keyword", new BasicDBObject("$regex", synonym));
					 * or.add(temp); }
					 */

					// BasicDBObject temp = new BasicDBObject("keyword", new
					// BasicDBObject("$regex", word));
					BasicDBObject temp = new BasicDBObject("keyword", word);
					or.add(temp);
				}

			}

			// search the keywords on my index
			BasicDBObject searchQuery = new BasicDBObject("$or", or);
			LOG.info("Shark gets data set from database..." + searchQuery.toString());

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

			LOG.info("Get resources and calc krm....");
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
					LOG.info(krm + " >= " + thKrm);
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
			System.out.println("SHARK gets relevant resources..." + searchRR.toString());
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
				LOG.info("sdf " + sdf);
				float rm = ((1.0f - 0.6f) * krm) + (0.6f * sdf);
				rmMap.put(documentKey, rm);
				LOG.info("RM:" + rm + ">=" + thRm);
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

			System.out.println("SHARK gets dataset from database..." + searchQuery.toString());
			//
			// rmList is the final List of resources!!!
			final ArrayList<Document> rmList = new ArrayList<Document>();
			iterable = dataset.find(searchQuery);
			System.out.println("searchQuery find..." + dataset.count(searchQuery));

			iterable.forEach(new Block<Document>() {
				public void apply(final Document document) {
					rmList.add(document);

				}
			});

			// before return split check socrata case
			for (Document doc : rmList) {
				doc.append("rm", rmMap.get(doc.get("_id")));
				doc.append("krm", krmMap.get(doc.get("_id")));
				doc.append("keywords", keywords);

				// if socrata split in set by 5000 record
				if (doc.containsKey("datainput_type") && doc.get("datainput_type")
						.equals("org.hammer.santamaria.mapper.dataset.SocrataDataSetInput")) {

					String socrataQuery = SocrataUtils.CreateWhereCondition(
							SparkSession.builder().appName("SHARK").getOrCreate(), doc.getString("_id"));

					if (socrataQuery.length() > 0 && doc.getString("dataset-type").equals("JSON")) {
						long count = SocrataUtils.CountPackageList(
								SparkSession.builder().appName("SHARK").getOrCreate(), doc.getString("url"),
								doc.getString("_id"));
						int offset = 0;

						if (count > 0) {
							while (offset < count) {
								Document tempDoc = new Document(doc);
								String tempUrl = SocrataUtils.GetUrl(
										SparkSession.builder().appName("SHARK").getOrCreate(), doc.getString("_id"),
										doc.getString("url"), offset, 1000, socrataQuery);
								tempDoc.replace("url", tempUrl);
								tempDoc.replace("_id", doc.get("_id") + "_" + offset);
								// save stat
								StatUtils.UpdateResultList(tempDoc, SparkSession.builder().appName("SHARK")
										.getOrCreate().sparkContext().conf().get("list-result"));
								offset = offset + 1000;
							}
						}
					}

				} else {
					// save stat
					StatUtils.UpdateResultList(doc, SparkSession.builder().appName("SHARK").getOrCreate().sparkContext()
							.conf().get("list-result"));
					returnList.add(doc);
				}
			}

		} catch (Exception ex) {
			LOG.error(ex.getMessage());
			LOG.debug(ex);
		}
		LOG.info("Shark find " + returnList.size());
		return returnList;
	}

	/**
	 * Create a new query
	 * 
	 * @param q
	 * @param arrayList
	 * @return
	 */
	private String getQuery(String q, List<Term[]> arrayList) {
		for (Term[] k : arrayList) {
			if (!k[0].getTerm().equals("select") && !k[0].getTerm().equals("where") && !k[0].getTerm().equals("from")
					&& !k[0].getTerm().equals("label1") && !k[0].getTerm().equals("value")
					&& !k[0].getTerm().equals("instance1") && !k[0].getTerm().equals("instance")
					&& !k[0].getTerm().equals("label")) {
				q = q.replaceAll(k[0].getTerm(), k[1].getTerm());
			}
		}
		return q;
	}

}
