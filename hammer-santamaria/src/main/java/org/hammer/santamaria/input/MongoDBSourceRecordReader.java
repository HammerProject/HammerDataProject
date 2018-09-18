package org.hammer.santamaria.input;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.hammer.santamaria.splitter.BaseDataSourceRecordReader;
import org.hammer.santamaria.splitter.DataSourceSplit;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

/**
 * MongoDB Record Reader
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class MongoDBSourceRecordReader extends BaseDataSourceRecordReader {

	
	private static final Log LOG = LogFactory.getLog(MongoDBSourceRecordReader.class);

	/**
	 * Output from CKAN source
	 */
	private String output = "";

	/**
	 * Data set
	 */
	private ArrayList<String> dataset = new ArrayList<String>();

	public MongoDBSourceRecordReader(final DataSourceSplit split) {
		super(split);
	}

	@Override
	public void close() {

	}

	@Override
	public Object getCurrentKey() {
		if (dataset.size() > 0 && seen < total) {
			return this.dataset.get((int) seen);
		}
		return NullWritable.get();
	}

	@Override
	public BSONObject getCurrentValue() {
		return current;
	}

	@Override
	public void initialize(final InputSplit split, final TaskAttemptContext context) {
		LOG.info("SANTA MARIA RECORD READER: Get collections list from MongoDB");
		this.total = 0;
		this.seen = 0;
		this.dataset = new ArrayList<String>();
		this.getPackageList();
		this.total = this.dataset.size();
	}

	@Override
	public boolean nextKeyValue() {

		if (seen < total) {
			LOG.info("Read " + (seen + 1) + " documents from (total " + total + ") :");
			LOG.info(split.getName() + " ----- " + this.dataset.get((int) seen));

			this.current = new BasicBSONObject();
			this.current.put("datasource", split.getName());
			this.current.put("dataset", this.dataset.get((int) seen));

			this.current.put("datainput_type", "org.hammer.santamaria.mapper.dataset.MongoDBDataSetInput");
			this.current.put("url", split.getUrl());
			this.current.put("action", split.getAction());

			seen++;
			return true;
		}

		return false;
	}

	/**
	 * Get data set from MongoDB repository
	 * 
	 * 
	 */
	private void getPackageList() {
		
		LOG.info("---> SANTA MARIA starts read collections");
		MongoClientURI connectionString = new MongoClientURI(split.getUrl());
		MongoClient mongoClient = new MongoClient(connectionString);
		MongoDatabase db = mongoClient.getDatabase(split.getAction());
		
		try {
			LOG.info("---> SANTA MARIA starts read collections --> " + split.getAction());

			for(String s : db.listCollectionNames()) {
				if(!s.contains("system")) {
					LOG.info("---> SANTA MARIA starts read collections ---> " + s);
					dataset.add(s);
				}
			}
			
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			mongoClient.close();
		}
	}

	/**
	 * @return the output
	 */
	public String getOutput() {
		return output;
	}

	/**
	 * @param output
	 *            the output to set
	 */
	public void setOutput(String output) {
		this.output = output;
	}

	public ArrayList<String> getDataset() {
		return dataset;
	}

	public void setDataset(ArrayList<String> dataset) {
		this.dataset = dataset;
	}
	
	
}
