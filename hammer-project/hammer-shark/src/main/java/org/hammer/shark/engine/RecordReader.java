package org.hammer.shark.engine;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.SparkSession;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.types.BasicBSONList;
import org.hammer.shark.utils.Config;
import org.hammer.shark.utils.JSON;
import org.hammer.shark.utils.SocrataUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Shark record reader
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Shark
 *
 */
public class RecordReader {

	private static final Log LOG = LogFactory.getLog(RecordReader.class);

	/**
	 * Searcg data
	 */
	@SuppressWarnings("unchecked")
	public static void search(DataSetSplit split) {
		SparkSession spark = SparkSession.builder().appName("SHARK").getOrCreate();
		List<Document> mylist = new ArrayList<>();

		try {
			LOG.info(split.getUrl());
			if (split.getDataSetType().equals("org.hammer.santamaria.mapper.dataset.SocrataDataSetInput")) {

				/*
				 * final BasicBSONList list = new BasicBSONList(); int count =
				 * SocrataUtils.CountPackageList(this.conf, split.getUrl(), split.getName());
				 * int offset = 0; count = (count > socrataRecordLimit) ? socrataRecordLimit :
				 * count;
				 * 
				 * while (offset < count) { BasicBSONList temp = (BasicBSONList)
				 * SocrataUtils.GetDataSet(this.conf, split.getName(), split.getUrl(), offset,
				 * 1000); list.addAll(temp); offset = offset + 1000; }
				 */
				BasicBSONList doc = (BasicBSONList) SocrataUtils.GetDataSet(spark, split.getName(), split.getUrl());
				for (Object obj : doc) {
					BasicDBObject bObj = (BasicDBObject) obj;
					bObj.put("dataset", split.getName());
					mylist.add(new Document(bObj.toMap()));
				}

			} else if (split.getDataSetType().equals("org.hammer.santamaria.mapper.dataset.MongoDBDataSetInput")) {
				MongoClientURI connectionString = new MongoClientURI(split.getUrl());
				MongoClient mongoClient = new MongoClient(connectionString);
				MongoDatabase db = mongoClient.getDatabase(connectionString.getDatabase());
				try {
					MongoCollection<Document> collection = db.getCollection(split.getDatasource());
					FindIterable<Document> record = collection.find();
					for (Document d : record) {
						d.put("dataset", split.getName());
						mylist.add(d);
					}


				} catch (Exception ex) {

				} finally {
					mongoClient.close();
				}

			} else {

				HttpClient client = new HttpClient();
				GetMethod method = null;

				try {
					method = new GetMethod(split.getUrl());

					method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
							new DefaultHttpMethodRetryHandler(3, false));
					method.setRequestHeader("User-Agent", "Hammer Project - Shark query");
					method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Shark query");

					int statusCode = client.executeMethod(method);

					if (statusCode != HttpStatus.SC_OK) {
						throw new Exception("Method failed: " + method.getStatusLine());
					}
					byte[] responseBody = method.getResponseBody();
					LOG.debug(new String(responseBody));

					if (split.getType().equals("JSON")) {
						BSONObject doc = (BSONObject) JSON.parse(new String(responseBody));

						// check if is a view + data object
						ArrayList<String> meta = new ArrayList<String>();
						BasicBSONList pList = null;

						for (String metaKey : doc.keySet()) {
							if (!meta.contains(metaKey.toLowerCase())) {
								meta.add(metaKey.toLowerCase());
							}
						}

						if ((meta.size() == 2) && (meta.contains("meta")) && (meta.contains("data"))) {
							pList = (BasicBSONList) ((BSONObject) ((BSONObject) doc.get("meta")).get("view"))
									.get("columns");
							meta = new ArrayList<String>();
							for (Object obj : pList) {
								BasicBSONObject pObj = (BasicBSONObject) obj;
								if (pObj.containsField("fieldName")) {
									meta.add(pObj.getString("fieldName").toLowerCase().replaceAll(":", ""));
								}
							}

							pList = (BasicBSONList) doc.get("data");
							for (Object obj : pList) {
								BasicDBList bObj = (BasicDBList) obj;
								BSONObject newObj = GetDataByItem(meta, bObj);
								newObj.put("dataset", split.getName());
								mylist.add(new Document(newObj.toMap()));
							}
						}

					} else if (split.getType().equals("CSV")) {

						// TODO next release

					} else if (split.getType().equals("XML")) {
						throw new Exception("Hammer Shark datatype not know " + split.getType());
					} else {
						throw new Exception("Hammer Shark datatype not know " + split.getType());
					}

				} catch (Exception e) {
					LOG.error(e);
				} finally {
					method.releaseConnection();
				}

			}

		} catch (Exception e) {
			LOG.error(e);
		}

		// save data to temp table

		MongoClient mongo = null;
		MongoDatabase db = null;
		try {
			MongoClientURI outputURI = new MongoClientURI(
					Config.getInstance().getConfig().getString("spark.mongodb.output.uri"));
			mongo = new MongoClient(outputURI);
			db = mongo.getDatabase(outputURI.getDatabase());
			System.out.println("SHARK QUERY Create temp table record-temp-table");
			for (Document bo : mylist) {

				try {
					if(bo.containsKey("_id")) {
						Document delete = new Document();
						delete.append("_id", bo.get("_id"));
						db.getCollection("record_temp_table").deleteMany(delete);
					}
					db.getCollection("record_temp_table").insertOne(bo);
					
				} catch (Exception e) {
					LOG.debug(e);
					LOG.error("SHARK QUERY: Error writing from temporary file", e);
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}


	}

	/**
	 * Get data from bson obj not in key-value format
	 * 
	 * @param bObj
	 * @return
	 */
	private static BSONObject GetDataByItem(ArrayList<String> meta, BasicDBList bObj) {
		BSONObject newObj = new BasicBSONObject();
		int i = 0;
		for (String t : meta) {
			LOG.debug(t + " --- ");
			newObj.put(t, bObj.get(i));
			i++;
		}
		return newObj;
	}
}
