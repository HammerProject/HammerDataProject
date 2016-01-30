package org.hammer.santamaria.mapper.dataset;

import java.util.ArrayList;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;

/**
 * INPS data set reader
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class INPSDataSetInput implements DataSetInput {

	private static final Log LOG = LogFactory.getLog(INPSDataSetInput.class);

	@SuppressWarnings("unchecked")
	public BSONObject getDataSet(String url, String datasource, String id, BSONObject c) {
		BSONObject dataset = new BasicBSONObject();
		
		dataset.put("datasource", datasource);
		dataset.put("id", id);
		
		LOG.info(datasource + id);
		
		HttpClient client = new HttpClient();
		GetMethod method = new GetMethod(url);
		method.setRequestHeader("User-Agent", "Hammer Project - SantaMaria crawler");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - SantaMaria crawler");

		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		try {
			int statusCode = client.executeMethod(method);
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			LOG.debug(new String(responseBody));
			Document doc = Document.parse(new String(responseBody));
			dataset.put("title", doc.getString("name"));
			dataset.put("author", doc.getString("author"));
			dataset.put("author_email", doc.getString("author_email"));
			dataset.put("license_id", doc.getString("license_id"));
			dataset.put("description", doc.getString("notes"));

			ArrayList<String> tags = new ArrayList<String>();
			ArrayList<String> meta = new ArrayList<String>();

			boolean findJSON = false;
			ArrayList<Document> resources = (ArrayList<Document>) doc.get("resources");
			for (Document resource : resources) {
				if (resource.getString("format").toUpperCase().equals("JSON")) {
					findJSON = true;
					dataset.put("dataset-type", "JSON");
					dataset.put("url", resource.get("url"));
					dataset.put("created", resource.get("created"));
					dataset.put("revision_timestamp", resource.get("last_modified"));
					meta = this.getMetaByDocument(resource.get("url").toString());
				}
			}
			
			if(findJSON) {
				tags = (ArrayList<String>) doc.get("tags");
			
			}
			dataset.put("tags", tags);
			dataset.put("meta", meta);
			
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}
		LOG.debug(dataset.get("title"));

		return dataset;
	}
	
	
	/**
	 * Return meta from document for CKAN implementation
	 * 
	 * @param url
	 * @return
	 */
	public ArrayList<String> getMetaByDocument(String url) {
		ArrayList<String> meta = new ArrayList<String>();
		HttpClient client = new HttpClient();
		GetMethod method = new GetMethod(url);
		method.setRequestHeader("User-Agent", "Hammer Project - SantaMaria crawler");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - SantaMaria crawler");

		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		try {
			int statusCode = client.executeMethod(method);
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			
			if ((JSON.parse(new String(responseBody))) instanceof BasicBSONList) {
				BasicBSONList pList = (BasicBSONList) JSON.parse(new String(responseBody));
				if (pList.size() > 0 && pList.get(0) != null) {
					BSONObject temp = (BSONObject) pList.get(0);
					for (String metaKey : temp.keySet()) {
						if (!meta.contains(metaKey.toLowerCase())) {
							meta.add(metaKey.toLowerCase());
						}
					}
				}
			} else if ((JSON.parse(new String(responseBody))) instanceof BasicDBObject) {
				BasicDBObject temp = (BasicDBObject) JSON.parse(new String(responseBody));
				for (String metaKey : temp.keySet()) {
					if (!meta.contains(metaKey.toLowerCase())) {
						meta.add(metaKey.toLowerCase());
					}
				}

			}
			

		} catch (Exception e) {
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}

		return meta;
	}

}
