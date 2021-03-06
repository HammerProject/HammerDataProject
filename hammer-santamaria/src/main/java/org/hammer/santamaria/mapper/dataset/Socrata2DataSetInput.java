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
import org.bson.types.BasicBSONList;
import org.hammer.santamaria.mapper.dataset.utils.DSSUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;

/**
 * Socrata3 data set reader
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class Socrata2DataSetInput implements DataSetInput {

	private static final Log LOG = LogFactory.getLog(Socrata2DataSetInput.class);

	public BSONObject getDataSet(String url, String datasource, String id, BSONObject c) {
		
		BSONObject dataset = (BSONObject) c.get("dataset-obj");

		HttpClient client = new HttpClient();
		client.getHttpConnectionManager().getParams().setConnectionTimeout(3000);
		client.getHttpConnectionManager().getParams().setSoTimeout(2000);
		
		
		LOG.info(dataset.get("api-link") + "?$offset=0&$limit=1");
		GetMethod method = new GetMethod(dataset.get("api-link") + "?$offset=0&$limit=1");
		method.setRequestHeader("User-Agent", "Hammer Project - SantaMaria crawler");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - SantaMaria crawler");
		
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		try {
			int statusCode = client.executeMethod(method);
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			ArrayList<String> meta = new ArrayList<String>();
			ArrayList<String> other_tags = new ArrayList<String>();

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
			
			if(dataset.keySet().contains("author")) other_tags.add(dataset.get("author").toString());
			if(dataset.keySet().contains("title")) other_tags.addAll(DSSUtils.GetKeyWordsFromText(dataset.get("title").toString()));
			if(dataset.keySet().contains("description")) other_tags.addAll(DSSUtils.GetKeyWordsFromText(dataset.get("description").toString()));

			dataset.put("dataset-type", "JSON");
			dataset.put("url", dataset.get("api-link"));
			dataset.put("meta", meta);
			dataset.put("other_tags", other_tags);

		} catch (Exception e) {
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}

		return dataset;
	}

}
