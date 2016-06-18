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
 * Read the list of Data Set (from keywords) and create an input-split for every
 * data set
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Colombo
 *
 */
public class DataSetSplitter extends MongoSplitter {

	/**
	 * Log
	 */
	private static final Log LOG = LogFactory.getLog(DataSetSplitter.class);

	/**
	 * Build a Data Set Splitter
	 */
	public DataSetSplitter() {
	}

	/**
	 * Build a data set splitter by configuration
	 * 
	 * @param conf
	 */
	public DataSetSplitter(final Configuration conf) {
		super(conf);
	}

	@Override
	public List<InputSplit> calculateSplits() throws SplitFailedException {
		final HashMap<String, Keyword> kwIndex = StatUtils.GetMyIndex(getConfiguration());
		LOG.info("---> Calculate INPUTSPLIT FOR DATASET");
		MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
		List<InputSplit> splits = new ArrayList<InputSplit>();
		LOG.debug("---> Colombo calculating splits for " + inputURI);

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
					if (sim > 0.90) {
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

		// esexute the getSetList function for every fuzzy query
		// the function return the list of the resources that match with the query
		// we store the data into a dataset map
		// for eliminate the duplicate resource
		Map<String, Document> dataSet = new HashMap<String, Document>();
		for (String key : qList.keySet()) {
			List<Document> temp = getSetList(qList.get(key), key);

			for (Document t : temp) {
				String documentKey = t.getString("_id");
				dataSet.put(documentKey, t);
			}
		}

		LOG.info("!!!!! FUZZY SEARCH has found " + dataSet.size() + " RESOURCES !!!!!");
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
	 * Contacts the db and builds a map of each set by keyword
	 * 
	 * 
	 * @return
	 */
	protected ArrayList<Document> getSetList(QueryGraph q, String keywords) {

		MongoClient mongo = null;
		final ArrayList<Document> returnList = new ArrayList<Document>();
		MongoDatabase db = null;
		float thKrm = Float.parseFloat(getConfiguration().get("thKrm"));
		float thRm = Float.parseFloat(getConfiguration().get("thRm"));

		try {

			MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());
			// create the table for the result
			// and clean
			BSONObject statObj = new BasicBSONObject();
			statObj.put("type", "clean");
			StatUtils.SaveStat(getConfiguration(), statObj);

			// connection with dataset and index collection of mongodb
			MongoCollection<Document> dataset = db.getCollection(inputURI.getCollection());
			MongoCollection<Document> index = db.getCollection("index");

			// now search the keywords or the labels on the index (with or)
			StringTokenizer st = new StringTokenizer(keywords, ";");

			BasicDBList or = new BasicDBList();
			while (st.hasMoreElements()) {
				String word = st.nextToken().trim().toLowerCase();
				if (word.trim().length() > 2) {
					/*
					 * ArrayList<String> synonyms =
					 * ThesaurusUtils.Get(getConfiguration().get("thesaurus.url"
					 * ), word, getConfiguration().get("thesaurus.lang"),
					 * getConfiguration().get("thesaurus.key"), "json");
					 * synonyms.add(word); for (String synonym : synonyms) {
					 * BasicDBObject temp = new BasicDBObject("keyword", new
					 * BasicDBObject("$regex", synonym)); or.add(temp); }
					 */

					// BasicDBObject temp = new BasicDBObject("keyword", new
					// BasicDBObject("$regex", word));
					BasicDBObject temp = new BasicDBObject("keyword", word);
					or.add(temp);
				}

			}

			// search the keywords on my index
			BasicDBObject searchQuery = new BasicDBObject("$or", or);
			LOG.debug("Colombo gets data set from database..." + searchQuery.toString());

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
			System.out.println("Colombo gets relevant resources..." + searchRR.toString());
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

			System.out.println("Colombo gets dataset from database..." + searchQuery.toString());
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
			for (Document doc : rmList) {

				// if socrata split in set by 5000 record
				if (doc.containsKey("datainput_type") && doc.get("datainput_type")
						.equals("org.hammer.santamaria.mapper.dataset.SocrataDataSetInput")) {

					String socrataQuery = SocrataUtils.CreateWhereCondition(this.getConfiguration(),
							doc.getString("_id"));

					if (socrataQuery.length() > 0 && doc.getString("dataset-type").equals("JSON")) {
						long count = SocrataUtils.CountPackageList(this.getConfiguration(), doc.getString("url"),
								doc.getString("_id"));
						int offset = 0;

						if (count > 0) {
							while (offset < count) {
								Document tempDoc = new Document(doc);
								String tempUrl = SocrataUtils.GetUrl(this.getConfiguration(), doc.getString("_id"),
										doc.getString("url"), offset, 1000, socrataQuery);
								tempDoc.replace("url", tempUrl);
								tempDoc.replace("_id", doc.get("_id") + "_" + offset);
								returnList.add(tempDoc);
								// save stat
								StatUtils.UpdateResultList(getConfiguration(), tempDoc);
								offset = offset + 1000;
							}
						}
					}

				} else {
            		// save stat
            		StatUtils.UpdateResultList(getConfiguration(), doc);
					returnList.add(doc);
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
		LOG.info("Colombo find " + returnList.size());
		return returnList;
	}

}