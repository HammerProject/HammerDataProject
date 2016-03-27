package org.hammer.santamaria.mapper.dataset;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
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
 * CKAN data set reader
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class CKANDataSetInput implements DataSetInput {

	/**
	 * Encode URI
	 * 
	 * @param s
	 * @return
	 */
	public static String EncodeURIComponent(String s) {
		String result;

		try {
			result = URLEncoder.encode(s, "UTF-8").replaceAll("\\+", "%20").replaceAll("\\%21", "!")
					.replaceAll("\\%27", "'").replaceAll("\\%28", "(").replaceAll("\\%29", ")")
					.replaceAll("\\%7E", "~");
		} catch (UnsupportedEncodingException e) {
			result = s;
		}

		return result;
	}
	
	public static final String PACKAGE_GET = "/package_show?id=";

	private static final Log LOG = LogFactory.getLog(CKANDataSetInput.class);

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public BSONObject getDataSet(String url, String datasource, String id, BSONObject c) {
		BSONObject dataset = new BasicBSONObject();
		dataset.put("datasource", datasource);
		dataset.put("id", id);
		HttpClient client = new HttpClient();
		String sId = EncodeURIComponent(id);
		GetMethod method = new GetMethod(url + PACKAGE_GET + sId);
		
		method.setRequestHeader("User-Agent", "Hammer Project - SantaMaria crawler");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - SantaMaria crawler");

		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		try {
			int statusCode = client.executeMethod(method);
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();

			Document doc = Document.parse(new String(responseBody));

			/*
			 * TODO CHECK VERSIONE CKAN
			 * 
			 * 
			 */
			if (doc.containsKey("result")) {
				Document result = new Document();
				LOG.info("id " + id);;
				LOG.info(doc.get("result").getClass().toString());
				if(!( doc.get("result") instanceof  Document)) {
					result = (Document) ((ArrayList) doc.get("result")).get(0);
					LOG.info("!!! list !!!!");
				} else {
					result = (Document) doc.get("result");
					LOG.info("!!! result !!!!");

				}
				
				dataset.put("title", result.get("title"));
				dataset.put("author", result.get("author"));
				dataset.put("author_email", result.get("author_email"));
				dataset.put("license_id", result.get("license_id"));

				boolean findJSON = false;
				ArrayList<String> tags = new ArrayList<String>();
				ArrayList<String> meta = new ArrayList<String>();


				ArrayList<Document> resources = (ArrayList<Document>) result.get("resources");
				for (Document resource : resources) {
					if (resource.getString("format").toUpperCase().equals("JSON")) {
						findJSON = true;
						dataset.put("dataset-type", "JSON");
						dataset.put("url", resource.get("url"));
						dataset.put("created", resource.get("created"));
						dataset.put("description", resource.get("description"));
						dataset.put("revision_timestamp", resource.get("revision_timestamp"));
						meta = this.getMetaByDocument(resource.get("url").toString());
					}
				}

				if (findJSON) {
					ArrayList<Document> tagsFromCKAN = (ArrayList<Document>) result.get("tags");
					for (Document tag : tagsFromCKAN) {
						if (tag.getString("state").toUpperCase().equals("ACTIVE")) {
							tags.add(tag.getString("display_name").trim().toLowerCase());
						}
					}

				}
				dataset.put("tags", tags);
				dataset.put("meta", meta);

			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}
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
		client.getHttpConnectionManager().getParams().setConnectionTimeout(3000);
		client.getHttpConnectionManager().getParams().setSoTimeout(2000);

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
