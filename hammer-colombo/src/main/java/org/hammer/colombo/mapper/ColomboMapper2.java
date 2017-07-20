package org.hammer.colombo.mapper;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.hammer.colombo.splitter.ColomboRecordReader;
import org.hammer.colombo.utils.JSON;
import org.hammer.colombo.utils.StatUtils;

import com.mongodb.BasicDBList;
import com.mongodb.hadoop.io.BSONWritable;

/**
 * New Version of Colombo Mapper
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Colombo
 *
 */
public class ColomboMapper2 extends Mapper<Object, BSONObject, Text, BSONWritable> {

	public static final Log LOG = LogFactory.getLog(ColomboMapper2.class);

	private Configuration conf = null;

	@Override
	protected void setup(Mapper<Object, BSONObject, Text, BSONWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		LOG.info("SETUP MAPPER2 - Hammer Colombo Project");
		this.conf = context.getConfiguration();

	}

	@Override
	public void map(final Object pKey, final BSONObject pValue, final Context pContext)
			throws IOException, InterruptedException {
		LOG.debug("START COLOMBO MAPPER " + pKey + " --- " + pValue);

		if (pValue != null) {
			LOG.debug("START COLOMBO MAPPER - Dataset " + pKey + " --- " + pValue.hashCode());

			
			if (conf.get("search-mode").equals("download")) {
				pContext.write(new Text((String) "size"), new BSONWritable(pValue));

			} else {
				// first case pValue is a list of record

				if (pValue instanceof BasicBSONList) {
					// save stat
					BSONObject statObj = new BasicBSONObject();
					statObj.put("type", "resource");
					statObj.put("name", (String) pKey);
					statObj.put("count", ((BasicBSONList) pValue).toMap().size());
					StatUtils.SaveStat(this.conf, statObj);

					BasicBSONList pList = (BasicBSONList) pValue;
					// for each record we store a key-value
					// - key = column-value
					// - value = the record
					for (Object pObj : pList) {

						if (pObj instanceof BSONObject) {
							BSONObject bsonObj = (BSONObject) pObj;
							bsonObj.put("datasource_id", (String) pKey);
							for (Object column : bsonObj.keySet()) {
								String columnName = (String) column;
								Text key = new Text(columnName + "|" + bsonObj.get(columnName));
								pContext.write(key, new BSONWritable(bsonObj));
							}
						} else if (pObj instanceof com.google.gson.internal.LinkedTreeMap) {
							@SuppressWarnings("rawtypes")
							com.google.gson.internal.LinkedTreeMap gObj = (com.google.gson.internal.LinkedTreeMap) pObj;

							BSONObject bsonObj = new BasicBSONObject();
							for (Object gKey : gObj.keySet()) {
								bsonObj.put(gKey.toString(), gObj.get(gKey));
							}
							bsonObj.put("datasource_id", (String) pKey);
							for (Object column : bsonObj.keySet()) {
								String columnName = (String) column;
								Text key = new Text(columnName + "|" + bsonObj.get(columnName));
								pContext.write(key, new BSONWritable(bsonObj));
							}
						}
					}
				}
				// second case pValue is a record
				else if (pValue instanceof BSONObject) {
					// save stat
					BSONObject statObj = new BasicBSONObject();
					statObj.put("type", "resource");
					statObj.put("name", (String) pKey);
					statObj.put("count", 1);
					StatUtils.SaveStat(this.conf, statObj);

					BSONObject bsonObj = (BSONObject) pValue;
					bsonObj.put("datasource_id", (String) pKey);
					for (Object column : bsonObj.keySet()) {
						String columnName = (String) column;
						Text key = new Text(columnName + "|" + bsonObj.get(columnName));
						pContext.write(key, new BSONWritable(bsonObj));
					}

				}

			}

		}
	}

	//
	// test function
	//
	public static void main(String[] pArgs) throws Exception {
		HttpClient client = new HttpClient();
		GetMethod method = null;

		try {
			method = new GetMethod("https://www.opendata.go.ke/api/views/2faz-jghi/rows.json?accessType=DOWNLOAD");

			method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
					new DefaultHttpMethodRetryHandler(3, false));
			method.setRequestHeader("User-Agent", "Hammer Project - Colombo query");
			method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Colombo query");

			int statusCode = client.executeMethod(method);

			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			LOG.debug(new String(responseBody));

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
				pList = (BasicBSONList) ((BSONObject) ((BSONObject) doc.get("meta")).get("view")).get("columns");
				meta = new ArrayList<String>();
				for (Object obj : pList) {
					BasicBSONObject pObj = (BasicBSONObject) obj;
					if (pObj.containsField("fieldName")) {
						meta.add(pObj.getString("fieldName").toLowerCase().replaceAll(":", ""));
					}
				}

				BasicBSONList newList = new BasicBSONList();
				pList = (BasicBSONList) doc.get("data");
				for (Object obj : pList) {
					BasicDBList bObj = (BasicDBList) obj;
					BSONObject newObj = ColomboRecordReader.GetDataByItem(meta, bObj);
					newList.add(newObj);
				}
				doc = newList;
				
				
				
				
				
			}
			
			
			if (doc instanceof BasicBSONList) {

				BasicBSONList newList = (BasicBSONList) doc;
				// for each record we store a key-value
				// - key = column-value
				// - value = the record
				for (Object pObj : newList) {

					if (pObj instanceof BSONObject) {
						BSONObject bsonObj = (BSONObject) pObj;
						for (Object column : bsonObj.keySet()) {
							String columnName = (String) column;
							LOG.info(columnName + "|" + bsonObj.get(columnName));
						}
					}
				}
			}
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}

	}

}
