package org.hammer.colombo.splitter;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.SparkSession;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.hammer.colombo.utils.RecursiveString;
import org.hammer.colombo.utils.SpaceUtils;
import org.hammer.colombo.utils.StatUtils;
import org.hammer.colombo.utils.Term;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.fuzzy.JaroWinkler;
import org.hammer.isabella.fuzzy.WordNetUtils;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Keyword;
import org.hammer.isabella.query.QueryGraph;

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.SplitFailedException;
import com.mongodb.hadoop.util.MongoConfigUtil;

import scala.Tuple2;

/**
 * 
 * Create the query for first map-reduce phase
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Colombo
 *
 */
public class QuerySplitter extends MongoSplitter {

	/**
	 * Log
	 */
	private static final Log LOG = LogFactory.getLog(QuerySplitter.class);

	/**
	 * Build a Query Splitter
	 */
	public QuerySplitter() {
	}

	/**
	 * Build a query splitter by configuration
	 * 
	 * @param conf
	 */
	public QuerySplitter(final Configuration conf) {
		super(conf);
	}

	/**
	 * Comparator for Term
	 */
	Comparator<Term> cmp = new Comparator<Term>() {
		public int compare(Term o1, Term o2) {
			return (o1.getWeigth() < o2.getWeigth()) ? 1 : ((o1.getWeigth() > o2.getWeigth()) ? -1 : 0);
		}
	};

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

	@Override
	public List<InputSplit> calculateSplits() throws SplitFailedException {
		final HashMap<String, Keyword> kwIndex = StatUtils.GetMyIndex(getConfiguration());
		LOG.info("---> Calculate INPUTSPLIT FOR QUERY");
		MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
		LOG.debug("---> Colombo Query calculating splits for " + inputURI);
		float thSim = Float.parseFloat(getConfiguration().get("thSim"));
		int thQuery = Integer.parseInt(getConfiguration().get("thQuery"));
		int maxSim = Integer.parseInt(getConfiguration().get("maxSim"));
		String wnHome = getConfiguration().get("wn-home") + "";
		String word2vecmodel = getConfiguration().get("word2vecmodel") + "";
		// init spark
		SparkSession spark = SparkSession
			      .builder()
			      .master("local")
			      .appName("Hammer Colombo")
			      .getOrCreate();
		LOG.info("SPARK INIT " + spark.version());
		Word2VecModel my_model = Word2VecModel.load(word2vecmodel);

		// create my query graph object
		// System.out.println(query);
		Isabella parser = new Isabella(new StringReader(getConfiguration().get("query-string")));
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
		 * q.calculateMyLabels(); getConfiguration().set("keywords",
		 * q.getMyLabels()); keywords = q.getMyLabels(); } else {
		 * q.labelSelection(); getConfiguration().set("keywords",
		 * q.getKeyWords()); keywords = q.getKeyWords();
		 * 
		 * StringTokenizer st = new StringTokenizer(keywords, ";"); while
		 * (st.hasMoreElements()) { String key =
		 * st.nextToken().trim().toLowerCase();
		 * 
		 * List<Term> tempList = new ArrayList<Term>(); for (String s :
		 * kwIndex.keySet()) { double sim = JaroWinkler.Apply(key,
		 * s.toLowerCase()); // set the degree threshold to custom value if (sim
		 * > thSim) { Term point = new Term(); point.setTerm(s.toLowerCase());
		 * point.setWeigth(sim); tempList.add(point);
		 * 
		 * } }
		 * 
		 * // add synset by word net Map<String, Double> mySynSet =
		 * WordNetUtils.MySynset(wnHome, key.toLowerCase());
		 * 
		 * 
		 * for (String s : mySynSet.keySet()) { if (kwIndex.containsKey(s)) {
		 * Term point = new Term(); point.setTerm(s.toLowerCase());
		 * point.setWeigth(mySynSet.get(s)); tempList.add(point); } }
		 * 
		 * // cut the queue to maxsim tempList.sort(cmp); if(tempList.size() >
		 * maxSim) { tempList = tempList.subList(0, maxSim); }
		 * 
		 * similarity.put(key, tempList); } }
		 */

		q.calculateMyLabels();
		getConfiguration().set("mylabels", q.getMyLabels());
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
			} catch(Exception ex) {
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

		LOG.info("------------------------------------------------------");
		LOG.info("---- Create all the combination per FUZZY SEARCH -----");
		// recursive call
		List<Term[]> optionsList = new ArrayList<Term[]>();
		List<List<Term[]>> beforePrunning = new ArrayList<List<Term[]>>();
		List<List<Term[]>> cases = new ArrayList<List<Term[]>>();

		// calculate all the combination
		RecursiveString.Recurse(optionsList, similarity, 0, beforePrunning);

		LOG.info("--- FUZZY SEARCH QUERY --> " + beforePrunning.size());
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
			cases = beforePrunning.subList(0, thQuery);
		} else {
			cases = beforePrunning;
		}

		//

		System.out.println("#############################################################################");
		LOG.info("--- FUZZY SEARCH QUERY AFTER PRUNNING --> " + cases.size());

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
		List<InputSplit> qSplit = new ArrayList<InputSplit>();
		// first we add the original query
		QuerySplit qOne = new QuerySplit();
		qOne.setKeywords(keywords);
		qOne.setQueryString(getConfiguration().get("query-string"));
		qSplit.add(qOne);

		for (int i = 0; i < cases.size(); i++) {
			LOG.debug("----> Query case " + (i + 1) + ": ");
			String keywordsCase = "";
			for (Term[] k : cases.get(i)) {
				LOG.debug(k[0] + "-" + k[1].getTerm() + ",");
				keywordsCase += ";" + k[1].getTerm();
			}
			String newQuery = getQuery(getConfiguration().get("query-string"), cases.get(i));
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
		StatUtils.SaveStat(getConfiguration(), statObj);

		// save stat
		statObj = new BasicBSONObject();
		statObj.put("type", "stat");
		statObj.put("record-total", 0);
		statObj.put("record-selected", 0);
		statObj.put("resource-count", 0);
		statObj.put("size", 0);
		statObj.put("fuzzy-query", qSplit.size());
		statObj.put("total-query", fuzzyQueryBefore);

		StatUtils.SaveStat(this.getConfiguration(), statObj);

		spark.stop();
		return qSplit;

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