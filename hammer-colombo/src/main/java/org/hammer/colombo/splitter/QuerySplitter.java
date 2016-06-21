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
import org.bson.Document;
import org.hammer.colombo.utils.RecursiveString;
import org.hammer.colombo.utils.SocrataUtils;
import org.hammer.colombo.utils.StatUtils;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.cc.util.QueryGraphCloner;
import org.hammer.isabella.fuzzy.JaroWinkler;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Keyword;
import org.hammer.isabella.query.QueryGraph;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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
		List<InputSplit> splits = new ArrayList<InputSplit>();
		LOG.debug("---> Colombo calculating splits for " + inputURI);
		float thSim = Float.parseFloat(getConfiguration().get("thSim"));
		// create my query graph object
		// System.out.println(query);
		Isabella parser = new Isabella(new StringReader(getConfiguration().get("query-string")));
		String keywords = "";
		Map<String, ArrayList<String>> similarity = new HashMap<String, ArrayList<String>>();
		QueryGraph q;
		try {
			q = parser.queryGraph();
			q.setIndex(kwIndex);
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

				ArrayList<String> tempList = new ArrayList<String>();
				for (String s : kwIndex.keySet()) {
					double sim = JaroWinkler.Apply(key, s.toLowerCase());
					// set the degree threshold to 90%
					if (sim > thSim) {
						tempList.add(s.toLowerCase());
					}
				}

				similarity.put(key, tempList);
			}
		}

		LOG.info("------------------------------------------------------");
		LOG.info("---- Create all the combination per FUZZY SEARCH -----");
		// recursive call
		ArrayList<String[]> optionsList = new ArrayList<String[]>();
		ArrayList<ArrayList<String[]>> cases = new ArrayList<ArrayList<String[]>>();
		
		// calculate all the combination
		RecursiveString.Recurse(optionsList, similarity, 0, cases);
		LOG.info("--- FUZZY SEARCH QUERY --> " + cases.size());

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
			for (String[] k : cases.get(i)) {
				LOG.debug(k[0] + "-" + k[1] + ",");
				keywordsCase += ";" + k[1];
			}

			try {
				QueryGraph temp = QueryGraphCloner.deepCopy(getConfiguration().get("query-string"), cases.get(i),
						kwIndex);
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
		statObj.put("fuzzy-query", qList.size());

		StatUtils.SaveStat(this.getConfiguration(), statObj);

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
			MongoCollection<Document> dataset = db.getCollection(inputURI.getCollection());
			MongoCollection<Document> index = db.getCollection("index");

			for (String key : qList.keySet()) {
				List<Document> temp = getSetList(qList.get(key), key, dataset, index);

				for (Document t : temp) {
					String documentKey = t.getString("_id");
					dataSet.put(documentKey, t);
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

		LOG.info("!!!!! FUZZY SEARCH has found " + dataSet.size() + " RESOURCES !!!!!");
		statObj = new BasicBSONObject();
		statObj.put("type", "stat");
		statObj.put("record-total", 0);
		statObj.put("record-selected", 0);
		statObj.put("resource-count", dataSet.size());
		statObj.put("size",  0);
		statObj.put("fuzzy-query", 0);
		StatUtils.SaveStat(this.getConfiguration(), statObj);
		//
		//
		//
		// pass the document to map function
		//
		//
		for (Document doc : dataSet.values()) {
			String key = doc.getString("_id");
			LOG.debug("---> found " + key + " - " + doc.getString("title"));
			DataSetSplit dsSplit = new DataSetSplit();
			if (getConfiguration().get("search-mode").equals("download")) {
				dsSplit.setName(key);
				if (doc.containsKey("url") && !doc.containsKey("remove")) {
					dsSplit.setUrl(doc.getString("url"));
					dsSplit.setType(doc.getString("dataset-type"));
					dsSplit.setDataSetType(doc.getString("datainput_type"));
					dsSplit.setDatasource(doc.getString("id"));
					splits.add(dsSplit);
				}
				/*
				 * if (doc.containsKey("resources")) { ArrayList<Document>
				 * resources = (ArrayList<Document>) doc.get("resources"); for
				 * (Document resource : resources) { String rKey = key + "_" +
				 * resource.getString("id"); dsSplit.setName(rKey);
				 * dsSplit.setUrl(resource.getString("url"));
				 * dsSplit.setType(doc.getString("dataset-type"));
				 * dsSplit.setDataSetType(doc.getString("datainput_type"));
				 * dsSplit.setDatasource(doc.getString("id"));
				 * splits.add(dsSplit);
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
					splits.add(dsSplit);
				}
			}
		}

		return splits;

	}


	/**
	 * Create a new query
	 * @param q
	 * @param arrayList
	 * @return
	 */
	private String getQuery(String q, ArrayList<String[]> arrayList) {
		for (String[] k : arrayList) {
			if (!k[0].equals("select") && !k[0].equals("where") && !k[0].equals("from") && !k[0].equals("label1")
					&& !k[0].equals("value") && !k[0].equals("instance1") && !k[0].equals("instance")
					&& !k[0].equals("label")) {
				q = q.replaceAll(k[0], k[1]);
			}
		}
		return q;
	}
}