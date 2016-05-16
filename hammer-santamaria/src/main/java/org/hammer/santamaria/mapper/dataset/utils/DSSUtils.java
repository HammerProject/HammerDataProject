package org.hammer.santamaria.mapper.dataset.utils;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.hammer.santamaria.utils.HtmlUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;


/**
 * DSS Utils tool kit
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class DSSUtils {

	/*
	 * LIMIT per input stream size to 1 MB to prevent heap size problem
	 */
	private static final int LIMIT = 1024 * 1024;// set to 1MB
	
	private static final Log LOG = LogFactory.getLog(DSSUtils.class);

	/**
	 * Return DSS name by meta
	 * 
	 * @param meta
	 * @return
	 */
	public static DSS CheckDSSByMeta(ArrayList<String> meta) {
		if(meta == null) {
			return DSS.UNDEFINED;
		}
		if((meta.size() == 2)&&(meta.contains("dataset_id"))&&(meta.contains("data"))) {
			return DSS.ALBANO_LAZIALE;
		}
		if((meta.size() == 2)&&(meta.contains("meta"))&&(meta.contains("data"))) {
			return DSS.OPENAFRICA_ORG;
		}
		
		return DSS.UNDEFINED;
	}
	
	/**
	 * Extract keywords from text
	 * 
	 * @param text
	 * @return
	 */
	public static ArrayList<String> GetKeyWordsFromText(String myText){
		String text = HtmlUtils.sanitize(myText);
		ArrayList<String> myKeys = new ArrayList<String>();
		if(text == null) {
			return myKeys;
		}
		StringTokenizer st = new StringTokenizer(text, " ");
		
		while (st.hasMoreElements()) {
			String tW = st.nextToken();
			StringTokenizer st1 = new StringTokenizer(tW, "-.,!_");
			while (st1.hasMoreElements()) {
				String word = st1.nextToken();
				if(word.length() >= 3) {
					myKeys.add(word.toLowerCase());
				}
			}
		}
		
		return myKeys;
	}
	
	
	/**
	 * Return meta from a resource
	 * 
	 * @param url
	 * @return
	 */
	public static ArrayList<String> GetMetaByResource(String url) {
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

			// Read the response body.
			// byte[] responseBody = method.getResponseBody();
			byte[] responseBody = null;
			InputStream instream = method.getResponseBodyAsStream();
			if (instream != null) {
				long contentLength = method.getResponseContentLength();
				if (contentLength < Integer.MAX_VALUE) { // guard below cast
															// from overflow
					ByteArrayOutputStream outstream = new ByteArrayOutputStream();
					byte[] buffer = new byte[1024];
					int len;
					int total = 0;

					while ((len = instream.read(buffer)) > 0 && total < LIMIT) {

						outstream.write(buffer, 0, len);
						total += len;
					}
					responseBody = outstream.toByteArray();
					outstream.close();

					// EntityUtils.consume(instream);
					method.abort();
					try {
						instream.close();
					} catch (Exception ex) {
						LOG.error(ex);
					}
				}
			}

			BasicBSONList pList = null;
			BSONObject temp = null;
			if ((JSON.parse(new String(responseBody))) instanceof BasicBSONList) {
				pList = (BasicBSONList) JSON.parse(new String(responseBody));
				if (pList.size() > 0 && pList.get(0) != null) {
					temp = (BSONObject) pList.get(0);
				}
			} else if ((JSON.parse(new String(responseBody))) instanceof BasicDBObject) {
				temp = (BasicDBObject) JSON.parse(new String(responseBody));
			}

			if (temp != null) {
				for (String metaKey : temp.keySet()) {
					if (!meta.contains(metaKey.toLowerCase())) {
						meta.add(metaKey.toLowerCase());
					}
				}
			}

			LOG.info("CHECK DSS by META ");
			for (String t : meta) {
				LOG.info(t + " --- ");
			}
			DSS dss = DSSUtils.CheckDSSByMeta(meta);
			if (dss.equals(DSS.ALBANO_LAZIALE)) {
				pList = (BasicBSONList) temp.get("data");
				if (pList.size() > 0 && pList.get(0) != null) {
					temp = (BSONObject) pList.get(0);
					meta = new ArrayList<String>();
					for (String metaKey : temp.keySet()) {
						if (!meta.contains(metaKey.toLowerCase())) {
							meta.add(metaKey.toLowerCase());
						}
					}
				}
				LOG.info("ALBANO LAZ. - CHECK DSS by META ");
				for (String t : meta) {
					LOG.debug(t + " --- ");
				}
			} else if (dss.equals(DSS.OPENAFRICA_ORG)) {
				pList = (BasicBSONList) ( (BSONObject) ((BSONObject) temp.get("meta")).get("view")).get("columns");
				meta = new ArrayList<String>();
				for(Object obj : pList) {
					BasicBSONObject pObj = (BasicBSONObject) obj;
					if (pObj.containsField("fieldName")) {
						meta.add(pObj.getString("fieldName").toLowerCase().replaceAll(":", ""));
					}
				}
				LOG.info("OPENAFRICA - CHECK DSS by META ");
				for (String t : meta) {
					LOG.debug(t + " --- ");
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
