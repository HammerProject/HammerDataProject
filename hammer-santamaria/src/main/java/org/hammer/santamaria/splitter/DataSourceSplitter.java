package org.hammer.santamaria.splitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;
import org.hammer.core.model.DataSource;
import org.hammer.santamaria.input.CKAN3BigSourceRecordReader;
import org.hammer.santamaria.input.CKANBigSourceRecordReader;

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
 * Read the list of Data Source and create an input-split for every source
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Santa Maria
 *
 */
public class DataSourceSplitter extends MongoSplitter {

	private static final Log LOG = LogFactory.getLog(DataSourceSplitter.class);

	public DataSourceSplitter() {
	}

	public DataSourceSplitter(final Configuration conf) {
		super(conf);
	}

	@Override
	public List<InputSplit> calculateSplits() throws SplitFailedException {
		System.out.println("Calculate INPUTSPLIT FOR DATASOURCE");
		MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
		List<InputSplit> splits = new ArrayList<InputSplit>();
		System.out.println("SingleMongoSplitter calculating splits for " + inputURI);
		Map<String, DataSource> dataSource = getSourceList();
		for (String key : dataSource.keySet()) {
			System.out.println("---> found " + key + " - " + dataSource.get(key).getUrl() + key + " - "
					+ dataSource.get(key).getType());
			DataSourceSplit dsSplit = new DataSourceSplit();
			dsSplit.setName(dataSource.get(key).getName());
			dsSplit.setUrl(dataSource.get(key).getUrl());
			dsSplit.setAction(dataSource.get(key).getAction());
			dsSplit.setType(dataSource.get(key).getType());
			splits.add(dsSplit);
		}
		return splits;
	}

	/**
	 * Contacts the db and builds a map of each source's name
	 * 
	 * 
	 * @return a Map of source url onto datasource
	 */
	protected Map<String, DataSource> getSourceList() {
		MongoClient mongo = null;
		final HashMap<String, DataSource> sourceMap = new HashMap<String, DataSource>();
		MongoDatabase db = null;
		System.out.println("Santa Maria gets data source from database...");
		final int LIMIT = this.getConfiguration().getInt("limit", 10000);
		try {
			MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			MongoCollection<Document> dataSource = db.getCollection(inputURI.getCollection());

			FindIterable<Document> iterable = dataSource.find();
			iterable.forEach(new Block<Document>() {

				public void apply(final Document document) {
					String name = document.getString("name");
					String url = document.getString("url");
					String action = document.getString("action");
					String type = document.getString("type");
					System.out.println("Find data source " + name + " --- " + url + " ---- " + type);
					if (type.equals("org.hammer.santamaria.input.CKANBigSourceRecordReader")) {
						action = document.getString("url") + CKANBigSourceRecordReader.ACTION + "0&limit="
								+ CKANBigSourceRecordReader.LIMIT;
						int count = CKANBigSourceRecordReader.GetCountByCkan(action);
						int total = count;
						int c = 1;
						while (count >= CKANBigSourceRecordReader.LIMIT) {
							DataSource ds = new DataSource();
							// ds.setName(name + " " + c);
							ds.setName(name);
							ds.setUrl(url);
							ds.setAction(action);
							ds.setType(type);
							sourceMap.put(name + " " + c, ds);
							c++;
							action = document.getString("url") + CKANBigSourceRecordReader.ACTION + total + "&limit="
									+ CKANBigSourceRecordReader.LIMIT;
							count = CKANBigSourceRecordReader.GetCountByCkan(action);
							total += count;
						}
						;

						DataSource ds = new DataSource();
						// ds.setName(name + " " + c);
						ds.setName(name);
						ds.setUrl(url);
						ds.setAction(action);
						ds.setType(type);
						sourceMap.put(name + " " + c, ds);
						c++;
					} else if (type.equals("org.hammer.santamaria.input.CKAN3BigSourceRecordReader")) {
						action = document.getString("url") + CKAN3BigSourceRecordReader.ACTION + "0&rows="
								+ CKAN3BigSourceRecordReader.LIMIT;
						String countUrl = document.getString("url") + CKAN3BigSourceRecordReader.ACTION + "0&rows=1";
						int count = CKAN3BigSourceRecordReader.GetCountByCkan3(countUrl);
						int total = CKAN3BigSourceRecordReader.LIMIT;
						int c = 1;
						while (total <= count && total <= LIMIT) {
							DataSource ds = new DataSource();
							ds.setName(name);
							ds.setUrl(url);
							ds.setAction(action);
							ds.setType(type);
							sourceMap.put(name + " " + c, ds);
							c++;
							action = document.getString("url") + CKAN3BigSourceRecordReader.ACTION + total + "&rows="
									+ CKAN3BigSourceRecordReader.LIMIT;
							total += CKAN3BigSourceRecordReader.LIMIT;
						}
						
						DataSource ds = new DataSource();
						// ds.setName(name + " " + c);
						ds.setName(name);
						ds.setUrl(url);
						ds.setAction(action);
						ds.setType(type);
						sourceMap.put(name + " " + c, ds);
						c++;
					} else {
						DataSource ds = new DataSource();
						ds.setName(name);
						ds.setUrl(url);
						ds.setAction(action);
						ds.setType(type);
						sourceMap.put(name, ds);
					}
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
		System.out.println("Santa Maria find " + sourceMap.size());
		return sourceMap;
	}

}