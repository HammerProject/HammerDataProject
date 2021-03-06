package org.hammer.colombo.splitter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;

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
 * Read the list of Data Set (from mongodb) and create an input-split for every
 * data set
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Colombo
 *
 */
public class DataSetSplitter2 extends MongoSplitter {

	/**
	 * Log
	 */
	private static final Log LOG = LogFactory.getLog(DataSetSplitter2.class);

	/**
	 * Build a Data Set Splitter
	 */
	public DataSetSplitter2() {
	}

	/**
	 * Build a data set splitter by configuration
	 * 
	 * @param conf
	 */
	public DataSetSplitter2(final Configuration conf) {
		super(conf);
	}

	@Override
	public List<InputSplit> calculateSplits() throws SplitFailedException {
		
		System.out.println("START-STOP --> START SCHEMA FITTING " + (new Date()));
		Date start = new Date();
		
		LOG.info("---> Calculate INPUTSPLIT FOR DATASET - 2phase");
		MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
		List<InputSplit> splits = new ArrayList<InputSplit>();
		LOG.info("---> Colombo calculating splits for - 2phase" + inputURI);

		// get the resource from db
		MongoClient mongo = null;
		MongoDatabase db = null;
		try {
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());
			// connection with resource table
			MongoCollection<Document> resource = db.getCollection(inputURI.getCollection());
			FindIterable<Document> iterable = resource.find();
			iterable.forEach(new Block<Document>() {

				public void apply(final Document doc) {
					
					LOG.debug("---> found " + doc.getString("_id") + " - " + doc.getString("title"));
					DataSetSplit dsSplit = new DataSetSplit();
					if (getConfiguration().get("search-mode").equals("download")) {
						dsSplit.setName(doc.getString("_id"));
						if (doc.containsKey("url") && !doc.containsKey("remove")) {
							dsSplit.setUrl(doc.getString("url"));
							dsSplit.setAction(doc.getString("action"));
							dsSplit.setDataset(doc.getString("dataset"));
							dsSplit.setType(doc.getString("dataset-type"));
							dsSplit.setDataSetType(doc.getString("datainput_type"));
							dsSplit.setDatasource(doc.getString("id"));
							splits.add(dsSplit);
						}
						
					} else {
						dsSplit.setName(doc.getString("_id"));
						if (doc.containsKey("url") && !doc.containsKey("remove")) {
							dsSplit.setUrl(doc.getString("url"));
							dsSplit.setAction(doc.getString("action"));
							dsSplit.setDataset(doc.getString("dataset"));
							dsSplit.setType(doc.getString("dataset-type"));
							dsSplit.setDataSetType(doc.getString("datainput_type"));
							dsSplit.setDatasource(doc.getString("id"));
							splits.add(dsSplit);
						}
					}

				}
			});
			

			
		} catch (Exception ex) {
			LOG.error(ex.getMessage());
			LOG.debug(ex);
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}

		LOG.info("!!!!! FUZZY SEARCH has found " + splits.size() + " RESOURCES !!!!!");
		
		System.out.println("START-STOP --> STOP SCHEMA FITTING " + (new Date()));
		long seconds = ((new Date()).getTime() - start.getTime())/1000;
		System.out.println("START-STOP --> TIME SCHEMA FITTING " + seconds);
		this.getConfiguration().set("start_time", (new Date()).getTime() + "");

		return splits;

	}


}