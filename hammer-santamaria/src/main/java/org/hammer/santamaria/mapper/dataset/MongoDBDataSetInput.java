package org.hammer.santamaria.mapper.dataset;

import java.util.ArrayList;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * MongoDB Dataset Input
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class MongoDBDataSetInput implements DataSetInput {

	private static final Log LOG = LogFactory.getLog(MongoDBDataSetInput.class);

	public BSONObject getDataSet(String url, String datasource, String id, BSONObject c) {
		BSONObject dataset = new BasicBSONObject();
		dataset.put("datasource", datasource);
		dataset.put("dataset", c.get("dataset"));
		dataset.put("id", id);
		dataset.put("dataset-type", "JSON");
		dataset.put("datainput_type", "org.hammer.santamaria.mapper.dataset.MongoDBDataSetInput");
		dataset.put("action", c.get("action"));
		dataset.put("url", url);

		String action = c.get("action").toString();

		LOG.info("---> id " + id);

		MongoClientURI connectionString = new MongoClientURI(url);
		MongoClient mongoClient = new MongoClient(connectionString);
		MongoDatabase db = mongoClient.getDatabase(action);

		LOG.info(
				"******************************************************************************************************");
		LOG.info(" ");
		LOG.info(url);
		LOG.info(" ");
		LOG.info(
				"******************************************************************************************************");

		try {
			LOG.info("---> SANTA MARIA starts read collection ---> " + c.get("dataset").toString());

			MongoCollection<Document> collection = db.getCollection(c.get("dataset").toString());

			if (collection != null) {
				dataset.put("title", c.get("dataset").toString());

				FindIterable<Document> cur = collection.find();
				Document dbo = cur.first();
				Set<String> s = dbo.keySet();
				LOG.info("Read fields for " + c.get("dataset").toString() + " --> " + s.size());
				
				
				
				ArrayList<String> tags = new ArrayList<String>();
				ArrayList<String> meta = new ArrayList<String>();
				meta.addAll(s);
				tags.addAll(s);
				
				ArrayList<String> other_tags = new ArrayList<String>();
				tags.add(c.get("dataset").toString());
				meta.add(c.get("dataset").toString());
				other_tags.add(c.get("dataset").toString());

				dataset.put("tags", tags);
				dataset.put("meta", meta);
				dataset.put("other_tags", other_tags);

			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e);
		} finally {
			mongoClient.close();
		}
		return dataset;
	}

}
