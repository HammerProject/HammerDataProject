package org.hammer_project.hammer_project.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;

public class HammerUtils {

	
	
	@SuppressWarnings("unchecked")
	public static ArrayList<BasicDBObject> GetFromTaino(String table) {
		ArrayList<BasicDBObject> docs = null;
		HttpClient client = new HttpClient();
		GetMethod method = new GetMethod("http://ma-ha-1.hammer.lan:8080/Taino?table=" + table);
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		method.setRequestHeader("User-Agent", "Hammer Project - Test Query");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Test Query");		
		try {
			int statusCode = client.executeMethod(method);
			
			
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			docs = (ArrayList<BasicDBObject>) JSON.parse(new String(responseBody));

			

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			method.releaseConnection();
		}
		return docs;
		
	}
	

	
	public static BasicDBObject MapQuest_GeoCoding(String indirizzo) throws UnsupportedEncodingException { 
		BasicDBObject pos = new BasicDBObject();
		HttpClient client = new HttpClient();
		System.out.println(indirizzo);
		GetMethod method = new GetMethod("http://open.mapquestapi.com/nominatim/v1/search.php?key=lFPoIn9umy96VUSamhF5lB34e6tMQjOJ&format=json&q=" + URLEncoder.encode(indirizzo.trim(), "UTF-8"));
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		method.setRequestHeader("User-Agent", "Hammer Project - Test Query");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Test Query");		
		try {
			int statusCode = client.executeMethod(method);
			
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			System.out.println(new String(responseBody));
			
			@SuppressWarnings("unchecked")
			ArrayList<BasicDBObject> data = (ArrayList<BasicDBObject>) JSON.parse(new String(responseBody));
			 			
			if(data!= null && data.size() > 0) {
				pos = data.get(0);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			method.releaseConnection();
		}
		return pos;
	  } 
	
	
	
	public static BasicDBObject Google_GeoCoding(String indirizzo) throws UnsupportedEncodingException { 
		String KEY = "AIzaSyDcWUYO3Akw091jF0z8MGjkSAVTs7ITj8c";
		BasicDBObject pos = new BasicDBObject();
		HttpClient client = new HttpClient();
		System.out.println(indirizzo);
		GetMethod method = new GetMethod("https://maps.googleapis.com/maps/api/geocode/json?key=" + KEY + "&address=" + URLEncoder.encode(indirizzo.trim(), "UTF-8"));
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		method.setRequestHeader("User-Agent", "Hammer Project - Test Query");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Test Query");		
		try {
			int statusCode = client.executeMethod(method);
			
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			//System.out.println(new String(responseBody));
			
			BasicDBObject data = (BasicDBObject) JSON.parse(new String(responseBody));
			if(data == null || !data.containsField("results")) {
				return null;
			}
			com.mongodb.BasicDBList result = (com.mongodb.BasicDBList) data.get("results");
			if(result == null || result.size() <= 0) {
				return null;
			}
			BasicDBObject geometry = (BasicDBObject) ((BasicDBObject) result.get(0)).get("geometry");
			if(geometry == null || !geometry.containsField("location")) {
				return null;
			}
			BasicDBObject tmp = (BasicDBObject) geometry.get("location");
			pos.append("lat", tmp.get("lat"));
			pos.append("lon", tmp.get("lng"));
			
			

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			method.releaseConnection();
		}
		return pos;
	  } 
	
	
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
	
	public static ArrayList<BasicDBObject> DCATtoCollection(String file) {
		ArrayList<BasicDBObject> docs = new ArrayList<BasicDBObject>();
		try {
			
			com.mongodb.BasicDBList list  = (com.mongodb.BasicDBList)(((com.mongodb.BasicDBObject)JSON.parse(new String(ReadFile(file)))).get("dataset"));
			for(Object obj : list) {
				BasicDBObject bObj = (BasicDBObject) obj;
				docs.add(bObj);
			}

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
