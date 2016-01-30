package org.hammer.pinta.splitter;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;

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
 * Read the list of Data Set and create an input-split for every
 * data set
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Pinta
 *
 */
public class PintaSplitter extends MongoSplitter {

	private static final Log LOG = LogFactory.getLog(PintaSplitter.class);

	public PintaSplitter() {
	}

	public PintaSplitter(final Configuration conf) {
		super(conf);
	}

	@Override
	public List<InputSplit> calculateSplits() throws SplitFailedException {
		System.out.println("Calculate INPUTSPLIT FOR PINTA");
		MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
		List<InputSplit> splits = new ArrayList<InputSplit>();
		
		System.out.println("PINTA calculating splits for " + inputURI);
		PintaSplit dsSplit = new PintaSplit();
		List<BasicDBObject> dataSet = getSetList();
		dsSplit.setId(dataSet.hashCode() + "");
		
		dsSplit.setDataset(dataSet);
		splits.add(dsSplit);
		return splits;
	}

	/**
	 * Contacts the db and builds a map
	 * 
	 * 
	 * @return
	 */
	protected List<BasicDBObject> getSetList() {

		MongoClient mongo = null;
		final List<BasicDBObject> setMap = new ArrayList<BasicDBObject>();
		MongoDatabase db = null;
		System.out.println("PINTA gets data set from database...");
		try {
			MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			MongoCollection<Document> dataSet = db.getCollection(inputURI.getCollection());
			FindIterable<Document> iterable = dataSet.find();

			iterable.forEach(new Block<Document>() {

				public void apply(final Document document) {
					System.out.println("--> PINTA Find data set " + document.getString("_id"));
					BasicDBObject obj = new BasicDBObject();
					obj.append("document", document.getString("_id"));
					obj.append("tags", document.get("tags"));
					obj.append("meta", document.get("meta"));
					setMap.add(obj);
				}
			});

		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}
		System.out.println("PINTA find " + setMap.size());
		return setMap;
	}

}