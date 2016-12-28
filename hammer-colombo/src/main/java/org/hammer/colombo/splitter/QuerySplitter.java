package org.hammer.colombo.splitter;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
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

	@Override
	public List<InputSplit> calculateSplits() throws SplitFailedException {
		final HashMap<String, Keyword> kwIndex = StatUtils.GetMyIndex(getConfiguration());
		LOG.info("---> Calculate INPUTSPLIT FOR QUERY");
		MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
		LOG.debug("---> Colombo Query calculating splits for " + inputURI);
		float thSim = Float.parseFloat(getConfiguration().get("thSim"));
		float thQuery = Float.parseFloat(getConfiguration().get("thQuery"));
		int maxSim = Integer.parseInt(getConfiguration().get("maxSim"));
		String wnHome = getConfiguration().get("wn-home") + "";

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

		if (getConfiguration().get("query-mode").equals("labels")) {
			q.calculateMyLabels();
			getConfiguration().set("keywords", q.getMyLabels());
			keywords = q.getMyLabels();
		} else {
			q.labelSelection();
			getConfiguration().set("keywords", q.getKeyWords());
			keywords = q.getKeyWords();

			StringTokenizer st = new StringTokenizer(keywords, ";");
			while (st.hasMoreElements()) {
				String key = st.nextToken().trim().toLowerCase();

				List<Term> tempList = new ArrayList<Term>();
				for (String s : kwIndex.keySet()) {
					double sim = JaroWinkler.Apply(key, s.toLowerCase());
					// set the degree threshold to custom value
					if (sim > thSim) {
						Term point = new Term();
						point.setTerm(s.toLowerCase());
						point.setWeigth(sim);
						tempList.add(point);
					}
				}
				
				
				
				// add synset by word net
				Map<String, String> mySynSet = WordNetUtils.MySynset(wnHome, key.toLowerCase());
				
				
				for (String s : mySynSet.keySet()) {
					if (kwIndex.containsKey(s)) {
						Term point = new Term();
						point.setTerm(s.toLowerCase());
						point.setWeigth(1.0d); // ????
						tempList.add(point);
					}
				}

				
				if(tempList.size() > maxSim) {
					tempList = tempList.subList(0, maxSim); // ????
				}
				
				similarity.put(key, tempList);
			}
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
		int fuzzyQueryBefore =  beforePrunning.size();

		// check the generate query with the main query and remove the major distance query
		for(List<Term[]> testq: beforePrunning) {
			double sim = SpaceUtils.cos(testq);
			if(sim >= thQuery) {
				cases.add(testq);
			}
		}
		//
		
		LOG.info("--- FUZZY SEARCH QUERY AFTER PRUNNING --> " + cases.size());

		
		// qSplit is the list of all query for fuzzy search
		List< InputSplit> qSplit = new ArrayList< InputSplit>();
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
			qSplit.add( newQ);
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


		return qSplit;

	}


	/**
	 * Create a new query
	 * @param q
	 * @param arrayList
	 * @return
	 */
	private String getQuery(String q, List<Term[]> arrayList) {
		for (Term[] k : arrayList) {
			if (!k[0].getTerm().equals("select") && !k[0].getTerm().equals("where") && !k[0].getTerm().equals("from") && !k[0].getTerm().equals("label1")
					&& !k[0].getTerm().equals("value") && !k[0].getTerm().equals("instance1") && !k[0].getTerm().equals("instance")
					&& !k[0].getTerm().equals("label")) {
				q = q.replaceAll(k[0].getTerm(), k[1].getTerm());
			}
		}
		return q;
	}
}