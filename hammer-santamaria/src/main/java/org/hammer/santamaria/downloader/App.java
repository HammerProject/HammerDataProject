package org.hammer.santamaria.downloader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;


/**
 * Downloader to get local resource for computing baseline
 *
 *
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Santa Maria
 *
 */
public class App {
	
	public static void main( String[] args )
    {
        System.out.println( "Hamme Project - Main" );
        try {
        	ArrayList<BasicDBObject> docs = App.GetFromFile("datasource/cityofnewyork.json");
    		System.out.println("Total resources " + docs.size());
    		int c = 1;
    		int count_record = 0;
    		for (BasicDBObject doc : docs) {
    			if(doc.containsField("api-link")) {
    				System.out.print(c + "/" + docs.size() + " - " + doc.get("api-link").toString());
    				count_record += GetFromUrl("datasource/temp_ny/" + doc.get("_id").toString() + ".json", doc.get("api-link").toString());
    				System.out.println(" ---> ok (" + count_record + ")");
    				
    			}
    			c++;
    		}
    		System.out.println("--> total record " + count_record);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
	
	/**
	 * Read from url and save to file
	 * @param filePath
	 * @param url
	 */
	public static int GetFromUrl(String filePath, String url) {
		File file = new File(filePath);
		if (file.exists())  {
			
			ArrayList<BasicDBObject> docs = GetFromFile(filePath);
			return docs.size();
		}
		
		
		//ArrayList<BasicDBObject> docs = null;
		HttpClient client = new HttpClient();
		GetMethod method = new GetMethod(url);
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		method.setRequestHeader("User-Agent", "Hammer Project - Performance Test");
		client.getHttpConnectionManager().getParams().setConnectionTimeout(3000);
		client.getHttpConnectionManager().getParams().setSoTimeout(2000);
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Performance Test");		
		try {
			int statusCode = client.executeMethod(method);
			
			
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			@SuppressWarnings("unchecked")
			ArrayList<BasicDBObject> docs = (ArrayList<BasicDBObject>) JSON.parse(new String(responseBody));
			SaveFile(filePath,responseBody);
			
			return docs.size();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			method.releaseConnection();
		}
		//return docs;
		return 0;
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
	
	/**
	 * Save to file
	 * @param filePath
	 * @param byteArray
	 */
	public static void SaveFile(String filePath, byte[] byteArray) {

		File file = new File(filePath);

		try (FileOutputStream fop = new FileOutputStream(file)) {

			// if file doesn't exists, then create it
			if (!file.exists()) {
				file.createNewFile();
				
				

			}

			
			fop.write(byteArray);
			fop.flush();
			fop.close();

		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
}
