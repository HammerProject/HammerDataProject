package org.hammer.colombo.splitter;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;
import org.hammer.colombo.utils.SocrataUtils;

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
		System.out.println("Calculate INPUTSPLIT FOR DATASET");
		MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
		List<InputSplit> splits = new ArrayList<InputSplit>();
		System.out.println("Colombo calculating splits for " + inputURI);
		
		List<Document> dataSet = getSetList();
		System.out.println("---> found " + dataSet.size() );
		for (Document doc : dataSet) {
			String key = doc.getString("_id");
			System.out.println("---> found " + key + " - " + doc.getString("title"));
			DataSetSplit dsSplit = new DataSetSplit();
			dsSplit.setName(key);
			if (doc.containsKey("url") && !doc.containsKey("remove")) {
				dsSplit.setUrl(doc.getString("url"));
				dsSplit.setType(doc.getString("dataset-type"));
				dsSplit.setDataSetType(doc.getString("datainput_type"));
				dsSplit.setDatasource(doc.getString("id"));
				splits.add(dsSplit);
			}
		}
		if(getConfiguration().getBoolean("only-count", true)) {
			return splits;
		} else {
			return  new ArrayList<InputSplit>();
		}
		
	}

	/**
	 * Contacts the db and builds a map of each set by keyword
	 * 
	 * 
	 * @return
	 */
	protected ArrayList<Document> getSetList() {

		MongoClient mongo = null;
		final ArrayList<Document> setList = new ArrayList<Document>();
		final ArrayList<Document> setListF = new ArrayList<Document>();

		MongoDatabase db = null;
		System.out.println("Colombo gets data set from database...");

		try {

			MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			if (db.getCollection(getConfiguration().get("list-result")) == null) {
				db.createCollection(getConfiguration().get("list-result"));
			}
			db.getCollection(getConfiguration().get("list-result")).deleteMany(new BasicDBObject());

			MongoCollection<Document> dataSet = db.getCollection(inputURI.getCollection());

			MongoCollection<Document> index = db.getCollection("index");
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

			BasicDBObject searchQuery = new BasicDBObject("$or", or);
			searchQuery.append("documents.dataset-type", new BasicDBObject("$regex", "JSON"));
			
			System.out.println("Colombo gets data set from database..." + searchQuery.toString());

			FindIterable<Document> indexS = index.find(searchQuery);

			final ArrayList<ArrayList<String>> total = new ArrayList<ArrayList<String>>();

			indexS.forEach(new Block<Document>() {

				public void apply(final Document document) {
					@SuppressWarnings("unchecked")
					ArrayList<String> docList = (ArrayList<String>) document.get("document");
					if (docList != null) {
						total.add(docList);
					}
				}
			});

			BasicDBList idSet = new BasicDBList();

			if (total.size() == 0) {
				throw new Exception("!!!!! ERROR NOTHING FOUND !!!!");
			}

			float limit = Float.parseFloat(getConfiguration().get("limit"));
			for (ArrayList<String> listId : total) {
				for (String key : listId) {
					float found = 0;
					for (ArrayList<String> lista : total) {
						if (lista.contains(key)) {
							found++;
						}
					}
					float p = ((float) found / (float) total.size());
					if (p >= limit) {
						BasicDBObject temp = new BasicDBObject("_id", key);
						idSet.add(temp);
					}
				}
			}

			BasicDBObject searchDataset = new BasicDBObject("$or", idSet);

			if (idSet.size() == 0) {
				throw new Exception("!!!!! ERROR NOTHING FOUND !!!!");
			}
			FindIterable<Document> iterable = dataSet.find(searchDataset);

			iterable.forEach(new Block<Document>() {

				public void apply(final Document document) {
					setList.add(document);

				}
			});

			for (Document doc : setList) {

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
								setListF.add(tempDoc);
								db.getCollection(getConfiguration().get("list-result")).insertOne(tempDoc);
								offset = offset + 1000;
							}
						}
					}

				} else {
					db.getCollection(getConfiguration().get("list-result")).insertOne(doc);
					setListF.add(doc);
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
		System.out.println("Colombo find " + setList.size());
		return setListF;
	}

}