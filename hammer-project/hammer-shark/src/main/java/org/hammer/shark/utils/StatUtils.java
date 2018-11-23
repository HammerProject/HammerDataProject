package org.hammer.shark.utils;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.BSONObject;
import org.bson.Document;
import org.hammer.isabella.query.Keyword;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * 
 * Stat utils
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Shark
 *
 */
public class StatUtils {

	private static final Log LOG = LogFactory.getLog(StatUtils.class);

	/**
	 * Get the inverted index
	 * 
	 * @param conf
	 * @param q
	 * @return
	 */
	public static HashMap<String, Keyword> GetMyIndex() {
		MongoClient mongo = null;
		final HashMap<String, Keyword> index = new HashMap<String, Keyword>();
		MongoDatabase db = null;
		LOG.debug("Select my index...");
		try {

			MongoClientURI inputURI = new MongoClientURI(
					Config.getInstance().getConfig().getString("spark.mongodb.input.uri"));
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			MongoCollection<Document> myIdx = db
					.getCollection(Config.getInstance().getConfig().getString("index-table") + "");
			final long totalResources = myIdx.count();
			LOG.info("TOTAL INDEX KEYWORDS ---> " + totalResources);
			FindIterable<Document> iterable = myIdx.find();

			iterable.forEach(new Block<Document>() {

				@SuppressWarnings("unchecked")
				public void apply(final Document document) {
					ArrayList<Document> docList = (ArrayList<Document>) document.get("documents");
					long mT = docList.size();
					Keyword k = new Keyword(document.getString("keyword"), totalResources, mT);

					index.put(document.getString("keyword"), k);

				}
			});

		} catch (Exception ex) {
			LOG.debug(ex);
			LOG.error(ex.getMessage());
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}
		return index;

	}

	/**
	 * Get the inverted index
	 * 
	 * @param conf
	 * @param q
	 * @return
	 */
	public static HashMap<String, HashMap<String, Keyword>> GetTreeIndex(HashMap<String, Keyword> index) {
		if (index != null) {
			index = GetMyIndex();
		}
		HashMap<String, HashMap<String, Keyword>> treeIndex = new HashMap<String, HashMap<String, Keyword>>();
		for(String s: index.keySet()) {
			String init = (s.length() > 3) ? s.substring(0, 3) : s;
			if(treeIndex.containsKey(init)) {
				treeIndex.get(init).put(s, index.get(s));
			} else {
				HashMap<String, Keyword> sub = new HashMap<String, Keyword>();
				sub.put(s, index.get(s));
				treeIndex.put(init, sub);
			}
		}
		return treeIndex;

	}

	/**
	 * 
	 * Save the stat of the query
	 * 
	 * @param list_result
	 *            the collection to save result
	 * @param stat
	 *            an obj of stat
	 */
	public static void SaveStat(BSONObject stat, String list_result, String stat_result) {
		MongoClient mongo = null;
		MongoDatabase db = null;
		try {

			MongoClientURI inputURI = new MongoClientURI(
					Config.getInstance().getConfig().getString("spark.mongodb.input.uri"));
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			// prepare the table for list and stat
			if (db.getCollection(list_result) == null) {
				db.createCollection(list_result);
			}
			if (db.getCollection(list_result) == null) {
				db.createCollection(list_result);
			}

			if (stat.containsField("type") && stat.get("type").equals("clean")) {
				db.getCollection(list_result).deleteMany(new BasicDBObject());
				db.getCollection(list_result).deleteMany(new BasicDBObject());
			}

			// save resource and count
			// pass an object with
			// type = resource
			// name = resource name
			// count = count of record
			if (stat.containsField("type") && stat.get("type").equals("resource")) {
				db.getCollection(list_result).findOneAndUpdate(new Document("_id", stat.get("name")),
						new Document("$set", new Document("size", stat.get("count"))));
			}

			// save resource and link
			// pass an object with
			// type = link
			// name = resource name
			// search_query
			// res_link
			if (stat.containsField("type") && stat.get("type").equals("link")) {
				db.getCollection(list_result).findOneAndUpdate(new Document("_id", stat.get("name")),
						new Document("$set", new Document("search_query", stat.get("search_query"))));
				db.getCollection(list_result).findOneAndUpdate(new Document("_id", stat.get("name")),
						new Document("$set", new Document("res_link", stat.get("res_link"))));

			}

			// save stat
			// pass an object with
			// type = stat
			// record-total
			// record-selected
			// resource-count
			// size
			//
			// if stat record exists we updated
			if (stat.containsField("type") && stat.get("type").equals("stat")) {
				FindIterable<Document> iterable = db.getCollection(stat_result).find();
				Document oldStat = iterable.first();

				long record_total = Long.parseLong("" + stat.get("record-total"));
				long record_selected = Long.parseLong("" + stat.get("record-selected"));
				long size = Long.parseLong("" + stat.get("size"));
				long resource_count = Long.parseLong("" + stat.get("resource-count"));
				long fuzzy_query = Long.parseLong("" + stat.get("fuzzy-query"));
				long total_query = Long.parseLong("" + stat.get("total-query"));

				if (oldStat != null) {
					record_total += oldStat.getLong("record-total");
					record_selected += oldStat.getLong("record-selected");
					size += oldStat.getLong("size");
					resource_count += oldStat.getLong("resource-count");
					fuzzy_query += oldStat.getLong("fuzzy-query");
					total_query += oldStat.getLong("total-query");

					db.getCollection(stat_result).findOneAndUpdate(new Document("_id", "stat"),
							new Document("$set", new Document("record-total", record_total)));
					db.getCollection(stat_result).findOneAndUpdate(new Document("_id", "stat"),
							new Document("$set", new Document("record-selected", record_selected)));
					db.getCollection(stat_result).findOneAndUpdate(new Document("_id", "stat"),
							new Document("$set", new Document("size", size)));
					db.getCollection(stat_result).findOneAndUpdate(new Document("_id", "stat"),
							new Document("$set", new Document("resource-count", resource_count)));
					db.getCollection(stat_result).findOneAndUpdate(new Document("_id", "stat"),
							new Document("$set", new Document("fuzzy-query", fuzzy_query)));
					db.getCollection(stat_result).findOneAndUpdate(new Document("_id", "stat"),
							new Document("$set", new Document("total-query", total_query)));
				} else {
					Document doc = new Document();
					doc.append("_id", "stat");
					doc.append("record-total", record_total);
					doc.append("record-selected", record_selected);
					doc.append("size", size);
					doc.append("resource-count", resource_count);
					doc.append("fuzzy-query", fuzzy_query);
					doc.append("total-query", total_query);
					db.getCollection(stat_result).insertOne(doc);
				}

			}

		} catch (Exception ex) {
			LOG.error(ex);
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}

	}

	/**
	 * 
	 * Save the stat of the query
	 * 
	 * @param doc
	 * @param stat
	 *            an obj of stat
	 */
	public static void UpdateResultList(Document doc, String list_result) {
		MongoClient mongo = null;
		MongoDatabase db = null;
		try {

			MongoClientURI inputURI = new MongoClientURI(
					Config.getInstance().getConfig().getString("spark.mongodb.input.uri"));
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			// prepare the table for list and stat
			if (db.getCollection(list_result) == null) {
				db.createCollection(list_result);
			}
			db.getCollection(list_result).deleteOne(doc);
			db.getCollection(list_result).insertOne(doc);

		} catch (Exception ex) {
			LOG.error(ex);
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}

	}
}
