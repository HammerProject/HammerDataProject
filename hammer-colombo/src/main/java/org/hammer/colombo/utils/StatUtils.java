package org.hammer.colombo.utils;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * 
 * Stat utils
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Pinta
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
	public static HashMap<String, Keyword> GetMyIndex(Configuration conf) {
		MongoClient mongo = null;
		final HashMap<String, Keyword> index = new HashMap<String, Keyword>();
		MongoDatabase db = null;
		LOG.debug("Select my index...");
		try {

			MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			MongoCollection<Document> myIdx = db.getCollection("index");
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
	 * 
	 * Save the stat of the query
	 * 
	 * @param conf
	 *            the hadoop configuration
	 * @param stat
	 *            an obj of stat
	 */
	public static void SaveStat(Configuration conf, BSONObject stat) {
		MongoClient mongo = null;
		MongoDatabase db = null;
		try {

			MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			// prepare the table for list and stat
			if (db.getCollection(conf.get("list-result")) == null) {
				db.createCollection(conf.get("list-result"));
			}
			if (db.getCollection(conf.get("stat-result")) == null) {
				db.createCollection(conf.get("stat-result"));
			}

			if (stat.containsField("type") && stat.get("type").equals("clean")) {
				db.getCollection(conf.get("list-result")).deleteMany(new BasicDBObject());
				db.getCollection(conf.get("stat-result")).deleteMany(new BasicDBObject());
			}

			// save resource and count
			// pass an object with
			// type = resource
			// name = resource name
			// count = count of record
			if (stat.containsField("type") && stat.get("type").equals("resource")) {
				db.getCollection(conf.get("list-result")).findOneAndUpdate(new Document("_id", stat.get("name")),
						new Document("$set", new Document("size", stat.get("count"))));
			}

			// save resource and link
			// pass an object with
			// type = link
			// name = resource name
			// search_query
			// res_link
			if (stat.containsField("type") && stat.get("type").equals("link")) {
				db.getCollection(conf.get("list-result")).findOneAndUpdate(new Document("_id", stat.get("name")),
						new Document("$set", new Document("search_query", stat.get("search_query"))));
				db.getCollection(conf.get("list-result")).findOneAndUpdate(new Document("_id", stat.get("name")),
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
				FindIterable<Document> iterable = db.getCollection(conf.get("stat-result")).find();
				Document oldStat = iterable.first();
				
				
				long  record_total = Long.parseLong("" + stat.get("record-total"));
				long  record_selected = Long.parseLong("" + stat.get("record-selected"));
				long  size =  Long.parseLong("" + stat.get("size"));
				long  resource_count = Long.parseLong("" + stat.get("resource-count"));
				long  fuzzy_query = Long.parseLong("" + stat.get("fuzzy-query"));
				
				if(oldStat != null ) {
					record_total += oldStat.getLong("record-total");
					record_selected += oldStat.getLong("record-selected");
					size += oldStat.getLong("size");
					resource_count += oldStat.getLong("resource-count");
					fuzzy_query += oldStat.getLong("fuzzy-query");
							
					db.getCollection(conf.get("stat-result")).findOneAndUpdate(new Document("_id", "stat"),
							new Document("$set", new Document("record-total", record_total)));
					db.getCollection(conf.get("stat-result")).findOneAndUpdate(new Document("_id", "stat"),
							new Document("$set", new Document("record-selected", record_selected)));
					db.getCollection(conf.get("stat-result")).findOneAndUpdate(new Document("_id", "stat"),
							new Document("$set", new Document("size", size)));
					db.getCollection(conf.get("stat-result")).findOneAndUpdate(new Document("_id", "stat"),
							new Document("$set", new Document("resource-count", resource_count)));
					db.getCollection(conf.get("stat-result")).findOneAndUpdate(new Document("_id", "stat"),
							new Document("$set", new Document("fuzzy-query", fuzzy_query)));
				} else {
					Document doc = new Document();
					doc.append("_id", "stat");
					doc.append("record-total", record_total);
					doc.append("record-selected", record_selected);
					doc.append("size", size);
					doc.append("resource-count", resource_count);
					doc.append("fuzzy-query", fuzzy_query);
					db.getCollection(conf.get("list-result")).insertOne(doc);
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

	}

	/**
	 * 
	 * Save the stat of the query
	 * 
	 * @param doc
	 * @param stat
	 *            an obj of stat
	 */
	public static void UpdateResultList(Configuration conf, Document doc) {
		MongoClient mongo = null;
		MongoDatabase db = null;
		try {

			MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			// prepare the table for list and stat
			if (db.getCollection(conf.get("list-result")) == null) {
				db.createCollection(conf.get("list-result"));
			}
			db.getCollection(conf.get("list-result")).deleteOne(doc);
			db.getCollection(conf.get("list-result")).insertOne(doc);

		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}

	}
}
