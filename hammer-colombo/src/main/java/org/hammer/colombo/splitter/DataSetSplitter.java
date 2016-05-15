package org.hammer.colombo.splitter;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;
import org.hammer.colombo.App;
import org.hammer.colombo.utils.SocrataUtils;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
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

	@SuppressWarnings("unchecked")
	@Override
	public List<InputSplit> calculateSplits() throws SplitFailedException {
		System.out.println("Calculate INPUTSPLIT FOR DATASET");
		MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
		List<InputSplit> splits = new ArrayList<InputSplit>();
		System.out.println("Colombo calculating splits for " + inputURI);

		List<Document> dataSet = getSetList();
		System.out.println("---> found !!!!!! " + dataSet.size());
		for (Document doc : dataSet) {
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
				if (doc.containsKey("resources")) {
					ArrayList<Document> resources = (ArrayList<Document>) doc.get("resources");
					for (Document resource : resources) {
						String rKey = key + "_" + resource.getString("id");
						dsSplit.setName(rKey);
						dsSplit.setUrl(resource.getString("url"));
						dsSplit.setType(doc.getString("dataset-type"));
						dsSplit.setDataSetType(doc.getString("datainput_type"));
						dsSplit.setDatasource(doc.getString("id"));
						splits.add(dsSplit);

					}
				}
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
		if (getConfiguration().getBoolean("only-count", true)) {
			new SplitFailedException("ONLY-COUNT set to true (only simulate input slits!!!)");
		}
		return splits;

	}

	/**
	 * Contacts the db and builds a map of each set by keyword
	 * 
	 * 
	 * @return
	 */
	protected ArrayList<Document> getSetList() {

		MongoClient mongo = null;
		final ArrayList<Document> returnList = new ArrayList<Document>();
		final HashMap<String, Keyword> kwIndex = App.GetMyIndex(getConfiguration());
		MongoDatabase db = null;
		System.out.println("Colombo gets data set from database...");
		float th = Float.parseFloat(getConfiguration().get("limit"));

		try {
			// create my query graph object
			// System.out.println(query);
			Isabella parser = new Isabella(new StringReader(getConfiguration().get("query-string")));

			QueryGraph q;
			try {
				q = parser.queryGraph();
			} catch (ParseException e) {
				throw new IOException(e);
			}
			q.setIndex(kwIndex);

			MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());
			// create the table for the result
			if (db.getCollection(getConfiguration().get("list-result")) == null) {
				db.createCollection(getConfiguration().get("list-result"));
			}
			db.getCollection(getConfiguration().get("list-result")).deleteMany(new BasicDBObject());

			// connection with dataset and index collection of mongodb
			MongoCollection<Document> dataset = db.getCollection(inputURI.getCollection());
			MongoCollection<Document> index = db.getCollection("index");

			// now search the keyword on the index (with or)
			StringTokenizer st = new StringTokenizer(getConfiguration().get("keywords"), ";");
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
			System.out.println("Colombo gets data set from database..." + searchQuery.toString());
			
			
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
				System.out.println(" Found keyword with resources --> " + kwFinded.size());
			}

			// now we find the relevant resources and calculate krm
			// krm = [0,1]
			// krm = number keyword match / total number of keyword
			//
			// the rSet contains the resources
			// the hast map krmMap contains the value of krm for each resources
			BasicDBList rSet = new BasicDBList();
			HashMap<String, Float> krmMap = new HashMap<String, Float>();

			System.out.println("Get resources and calc krm....");
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
					if (krm >= th) {
						BasicDBObject temp = new BasicDBObject("_id", key);
						if (!krmMap.containsKey(key)) {
							rSet.add(temp);
							krmMap.put(key, krm);
						}
					}
				}
			}

			System.out.println(" ----------------------------------------------> ");
			if (rSet.size() == 0) {
				throw new Exception("!!!!! ERROR NOTHING RESOURCE FOUND !!!!");
			} else {
				System.out.println(" --> fount relevant resources  " + rSet.size());
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
			// th in this case is set to 0.2

			// the rmMap contain the rm-value for each document_id
			//
			float thRm = 0.3f;
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
				throw new Exception("!!!!! ERROR NOTHING RELEVANT RESOURCES FOUND (with rm >= 0.3) !!!!");
			} else {
				System.out.println("--- > FOUND RELEVANT RESOURCES FOUND (with rm >= 0.3) " + idSet.size());
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
						int count = SocrataUtils.CountPackageList(this.getConfiguration(), doc.getString("url"),
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
								db.getCollection(getConfiguration().get("list-result")).insertOne(tempDoc);
								offset = offset + 1000;
							}
						}
					}

				} else {
					db.getCollection(getConfiguration().get("list-result")).insertOne(doc);
					returnList.add(doc);
				}
			}

		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}
		System.out.println("Colombo find " + returnList.size());
		return returnList;
	}

}