package org.hammer.colombo.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.FileRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;

import org.apache.commons.httpclient.params.HttpMethodParams;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;

/**
 * 
 * Upload documenti into Apache Solr
 * 
 * Apache Solr is used to create baseline to compare performance with Hammer
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Santa Maria
 *
 */
public class SolrUpdate {

	

	public static void main( String[] args )
    {
        System.out.println( "Hamme Project - Main" );
        try {
        	ArrayList<BasicDBObject> docs = SolrUpdate.GetFromFile("../hammer-santamaria/datasource/opendataafrica.json");
    		System.out.println("Total resources " + docs.size());
    		int c = 1;
    		for (BasicDBObject doc : docs) {
    			if(doc.containsField("url")) {
    				System.out.print(c + "/" + docs.size() + " - " + doc.get("_id").toString());
    				UpdateSorl("../hammer-santamaria/datasource/temp/" + doc.get("_id").toString() + ".json", doc.get("_id").toString(), doc.get("title").toString());
    				System.out.println(" ---> ok");
    				
    			}
    			c++;
    		}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
	
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
	
	/**
	 * Read from url and save to file
	 * @param filePath
	 * @param url
	 */
	public static void UpdateSorl(String filePath, String id, String title) {
		File file = new File(filePath);
		if (!file.exists()) return; else System.out.println("Found!");
		
		
		//ArrayList<BasicDBObject> docs = null;
		HttpClient client = new HttpClient();
		PostMethod method = new PostMethod("http://ma-ha-2:8983/solr/openafrica/update/extract?literal.id=" + id + "&literal.title=" + EncodeURIComponent(title) + "&commit=true");
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		method.setRequestHeader("User-Agent", "Hammer Project - Performance Test");
		client.getHttpConnectionManager().getParams().setConnectionTimeout(3000);
		client.getHttpConnectionManager().getParams().setSoTimeout(2000);
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Performance Test");		
		try {
			RequestEntity requestEntity = new FileRequestEntity(file, title);
			//method.setParameter("myfile", SolrUpdate.ReadFile(filePath));
			//RequestEntity requestEntity = new StringRequestEntity(SolrUpdate.ReadFile(filePath), "application/json", null);
			method.setRequestEntity(requestEntity);

			int statusCode = client.executeMethod(method);
			
			
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			System.out.println(new String(responseBody));
			

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			method.releaseConnection();
		}
		//return docs;
		
	}
	
	

	/**
	 * Get BasicDBObject from file
	 * @param file
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static ArrayList<BasicDBObject> GetFromFile(String file) {
		ArrayList<BasicDBObject> docs = null;
		try {
			
			docs = (ArrayList<BasicDBObject>) JSON.parse(new String(ReadFile(file)));

			

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
		return docs;
		
	}
	

	/**
	 * File to string funcions
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static String ReadFile(String fileName) throws IOException {
		BufferedReader br = null;
		StringBuilder sb = null;
		try {
			br = new BufferedReader(new FileReader(fileName));
			sb = new StringBuilder();
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}
			return sb.toString();
		} finally {
			sb = null;
			if (br != null)
				br.close();
		}
	}
	
	
}
