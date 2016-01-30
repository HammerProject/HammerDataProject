package org.hammer.colombo.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

public class ThesaurusUtils {

	private static final Log LOG = LogFactory.getLog(ThesaurusUtils.class);
	
	public static ArrayList<String> Get(String url, String word, String language, String key, String output) throws UnsupportedEncodingException { 
		ArrayList<String> returnList = new ArrayList<String>();
		HttpClient client = new HttpClient();
		GetMethod method = new GetMethod(url +  "?word="+URLEncoder.encode(word, "UTF-8")+"&language="+language+"&key="+key+"&output="+output);
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		method.setRequestHeader("User-Agent", "Hammer Project - Colombo Query");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Colombo Query");		
		try {
			int statusCode = client.executeMethod(method);
			
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			LOG.info(new String(responseBody));
			Document doc = Document.parse(new String(responseBody));
			@SuppressWarnings("unchecked")
			ArrayList<Document> resources = (ArrayList<Document>) doc.get("response");
			
			for(Document list : resources) {
				String synonyms = (((Document)list.get("list"))).getString("synonyms");
				StringTokenizer st = new StringTokenizer(synonyms, "|");
				while (st.hasMoreElements()) {
					String sym = st.nextElement().toString().trim().toLowerCase();
					if(!returnList.contains(sym)) {
						returnList.add(sym);
					}
				}
			}

		} catch (Exception e) {
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}
		return returnList;
	  } 
}
