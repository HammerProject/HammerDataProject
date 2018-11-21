package org.hammer.shark.query;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.explode;

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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
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
import org.hammer.shark.utils.SpaceUtils;
import org.hammer.shark.utils.StatUtils;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.splitter.SplitFailedException;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;

import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

/**
 * SHARK Query
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - SHARK
 *
 */
public class SharkQuery2 implements Serializable {

	public SharkQuery2(SparkSession spark) {

	}

	public static final Log LOG = LogFactory.getLog(SharkQuery2.class);

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
		float cosSim = Float.parseFloat(spark.sparkContext().conf().get("cosSim"));

		int maxSim = Integer.parseInt(spark.sparkContext().conf().get("maxSim"));
		//String searchMode = spark.sparkContext().conf().get("search-mode");
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
		transform(ds, spark, resTable, wnHome, kwIndex, thKrm, thRm);
		/*
		 * List<Document> myList = qSplit.parallelStream().map(x -> { return mapper(x,
		 * wnHome, kwIndex, searchMode, thKrm, thRm); }).collect(Collectors.toList());
		 */

		System.out.println("START-STOP --> STOP Keyword Selection " + (new Date()));
		seconds = ((new Date()).getTime() - start.getTime());
		System.out.println("START-STOP --> TIME Keyword Selection (ms) " + seconds);
		start = new Date();

		System.out.println("START-STOP --> START VSM Data Set Retrieval " + (new Date()));

		/*
		 * MongoClient mongo = null; MongoDatabase db = null; try { MongoClientURI
		 * outputURI = new MongoClientURI(
		 * Config.getInstance().getConfig().getString("spark.mongodb.output.uri"));
		 * mongo = new MongoClient(outputURI); db =
		 * mongo.getDatabase(outputURI.getDatabase());
		 * System.out.println("SHARK QUERY Create temp table " +
		 * spark.sparkContext().conf().get("resource-table")); if
		 * (db.getCollection(spark.sparkContext().conf().get("resource-table")) != null)
		 * { db.getCollection(spark.sparkContext().conf().get("resource-table")).drop();
		 * } db.createCollection(spark.sparkContext().conf().get("resource-table"));
		 * MongoCollection<Document> collection =
		 * db.getCollection(spark.sparkContext().conf().get("resource-table"));
		 * 
		 * int inserted = 0; for(Document bo: myList) { if(bo!= null) { try {
		 * 
		 * // SAVE LIST OF RESOURCE TO MONGO DB BasicDBObject searchQuery = new
		 * BasicDBObject().append("_id", bo.get("_id")); long c =
		 * collection.count(searchQuery); if (c == 0) { collection.insertOne(bo);
		 * inserted++; }
		 * 
		 * } catch (Exception e) { LOG.debug(e);
		 * LOG.error("SHARK QUERY: Error reading from temporary file", e); throw new
		 * IOException(e); } } }
		 * 
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

	private static void transform(Dataset<QuerySplit> ds, SparkSession spark, String resourceTable, String wnHome,
			HashMap<String, Keyword> kwIndex, float thKrm, float thRm) {
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("database", "hammer");
		readOverrides.put("collection", Config.getInstance().getConfig().getString("index-table"));
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

		Dataset<Row> index = MongoSpark.load(jsc, readConfig).toDF().cache();

		readOverrides = new HashMap<String, String>();
		readOverrides.put("database", "hammer");
		readOverrides.put("collection", spark.sparkContext().conf().get("dataset-table"));
		readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
		Dataset<Row> dataset = MongoSpark.load(jsc, readConfig).toDF().cache();

		
		Dataset<Row> krm = ds.flatMap(p -> {
			String queryString = p.getQueryString();
			Isabella parser = new Isabella(new StringReader(queryString));
			QueryGraph query;
			try {
				query = parser.queryGraph();
				query.setIndex(kwIndex);
				query.setWnHome(wnHome);
			} catch (ParseException e) {
				return null;
			}

			// select label
			query.labelSelection();
			for (IsabellaError err : parser.getErrors().values()) {
				LOG.error(err.toString());
			}
			if (parser.getErrors().size() != 0) {
				return null;
			}
			// end of the query parse
			// return couple of keywords / whereWeight
			String keywords = query.getKeyWords();
			final Float w = query.getWeightWhere();
			String[] s = keywords.split(";");
			List<Tuple4<String, String, Float, String>> myList = new ArrayList<>();
			for(String k : s) {
				myList.add(new Tuple4<>(queryString, k, w, keywords));
			}
			
			return myList.iterator();

		}, Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.FLOAT(), Encoders.STRING()))
				.filter(t -> (t!=null))
				.toDF("query", "key","w","keywords").filter(t -> (t!=null))
				.toDF("query", "key","w","keywords")
				.join(index, col("key").equalTo(index.col("_id")), "inner").select(col("query"), col("key"), col("w"), col("keywords"), explode(col("documents.document")).as("documentKey")).cache();
		
		Dataset<Row> queryDocTotalCount = krm.groupBy("query").agg(count("documentKey").as("totalCount")).cache();
		Dataset<Row> queryDoclocalCount = krm.groupBy("query","documentKey").agg(count("documentKey").as("docCount")).cache();
		// calc krm
		Dataset<Row> docKrmList = queryDocTotalCount.join(queryDoclocalCount, queryDocTotalCount.col("query").equalTo(queryDoclocalCount.col("query")), "inner").map(r -> {
			long totalCount = r.getLong(r.fieldIndex("totalCount"));
			long docCount = r.getLong(r.fieldIndex("docCount"));
			float w = r.getFloat(r.fieldIndex("w"));
			String keywords = r.getString(r.fieldIndex("keywords"));
			String query = r.getString(r.fieldIndex("query"));
			String key = r.getString(r.fieldIndex("documentKey"));
			float krmvalue = ((float) docCount / (float) totalCount);
			return new Tuple5<>(key, krmvalue, w, keywords, query);
		}, Encoders.tuple(Encoders.STRING(), Encoders.FLOAT(), Encoders.FLOAT(), Encoders.STRING(),  Encoders.STRING())).filter(t -> (t._2() >= thKrm))
				.toDF("documentKey","krm", "w", "keywords", "query")
				.cache();
		
		// calc sdf and rm
		Dataset<Row> rmList = docKrmList.join(dataset, docKrmList.col("documentKey").equalTo(dataset.col("_id")), "inner").map(r -> {

			float w = r.getFloat(r.fieldIndex("w"));
			float krmValue = r.getFloat(r.fieldIndex("krm"));
			String keywords = r.getString(r.fieldIndex("keywords"));
			String queryString = r.getString(r.fieldIndex("query"));
			String documentKey = r.getString(r.fieldIndex("documentKey"));

			// recreate the query
			Isabella parser = new Isabella(new StringReader(queryString));
			QueryGraph query;
			try {
				query = parser.queryGraph();
				query.setIndex(kwIndex);
				query.setWnHome(wnHome);
			} catch (ParseException e) {
				return null;
			}

			// select label
			query.labelSelection();
			for (IsabellaError err : parser.getErrors().values()) {
				LOG.error(err.toString());
			}
			if (parser.getErrors().size() != 0) {
				return null;
			}
			final HashMap<String, Float> wWhere = query.getwWhere();
			float sdf = 0.0f;
			List<String> meta = new ArrayList<String>();
			if (r.schema().contains("meta") && r.get(r.fieldIndex("meta")) != null) {
				meta = r.getList(r.fieldIndex("meta"));
			}
			for (String k : meta) {
				sdf += (wWhere.containsKey(k)) ? wWhere.get(k) : 0.0f;
			}
			sdf = (float) sdf / (float) w;
			float rm = ((1.0f - 0.6f) * krmValue) + (0.6f * sdf);
			return new Tuple4<>(documentKey, rm, krmValue, keywords);
			
		}, Encoders.tuple(Encoders.STRING(), Encoders.FLOAT(),  Encoders.FLOAT(), Encoders.STRING()))
				.filter(t -> (t!=null))
				.filter(t -> t._2() >= thRm)
				.toDF("documentKey","rm", "krm", "keywords")
				.cache();
		
		Dataset<DS> finalDS = rmList.join(dataset, rmList.col("documentKey").equalTo(dataset.col("_id")), "inner").map(r -> {
			float krmValue = r.getFloat(r.fieldIndex("krm"));
			float rmValue = r.getFloat(r.fieldIndex("rm"));
			String keywords = r.getString(r.fieldIndex("keywords"));
			String documentKey = r.getString(r.fieldIndex("documentKey"));
			String datainput_type = r.getString(r.fieldIndex("datainput_type"));
			String dataset_type = r.getString(r.fieldIndex("dataset-type"));
			String url = r.getString(r.fieldIndex("url"));
			String id = r.getString(r.fieldIndex("id"));
			
			DS myDS = new DS();
			myDS.setKeywords(keywords);
			myDS.set_id(documentKey);
			myDS.setDatainput_type(datainput_type);
			myDS.setDataset_type(dataset_type);
			myDS.setId(id);
			myDS.setKrm(krmValue);
			myDS.setRm(rmValue);
			myDS.setUrl(url);
			

			return myDS;
		}, Encoders.bean(DS.class))
				.filter(t -> (t != null))
				.filter(t -> (t.getUrl() != null && t.getUrl().trim().length() > 0))
				.groupByKey(t -> t.get_id(), Encoders.STRING()).reduceGroups((a, b) -> {
					if (a.getRm() < b.getRm())
						return a;
					else
						return b;
				}).map(r -> {
					return r._2();
				}, Encoders.bean(DS.class));

		Map<String, String> writeOverrides = new HashMap<String, String>();
		writeOverrides.put("database", "hammer");
		writeOverrides.put("collection", resourceTable);
		WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

		MongoSpark.save(finalDS.write().mode(SaveMode.Overwrite), writeConfig);

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
