package org.hammer.colombo.utils;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;
import org.hammer.isabella.cc.util.IsabellaUtils;

public class PerformanceMetric {

	/**
	 * Log	
	 */
	private static final Log LOG = LogFactory.getLog(PerformanceMetric.class);
	
	/**
	 * Main
	 * @param pArgs
	 * @throws Exception
	 */
	public static void main(String[] pArgs) throws Exception {
		long totalRecord = 0;
		int countResources = 0;
		try {
			
			String response = IsabellaUtils.readFile("list.json");

				BasicBSONList list = (BasicBSONList) JSON.parse(new String(response));
				for(Object obj: list) {
					BSONObject bobj = (BSONObject) obj;
					countResources++;
					//System.out.println("Title:  " + bobj.get("title"));
					if (bobj.containsField("url") && !bobj.containsField("remove") && bobj.get("dataset-type").equals("JSON")) {
						
						String url = bobj.get("url") + "";
						
						totalRecord += CountRecord(url);
						
						System.out.println("Current record:  " + totalRecord);
						System.out.println("Current resources:  " + countResources);
						

					}
				}

			System.out.println(totalRecord);
		} catch (Exception e) {
			LOG.error(e);
		}
		System.out.println("******************************");
		System.out.println("Total record:  " + totalRecord);
		System.out.println("Total resources:  " + countResources);
	}
	
	/**
	 * Count record
	 * 
	 * @param url
	 * @return
	 * @throws Exception
	 */
	public static long CountRecord(String url) throws Exception {
		long record = 0;
		InputStream in = null;
		BufferedWriter br = null;
		OutputStream out = null;
		try {
			out = new FileOutputStream("temp.json");
			br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
			in = new BufferedInputStream(new URL(url).openStream());

			
			long total = IOUtils.copyLarge(in, out);
			String response = IsabellaUtils.readFile("temp.json");
			try {
				BSONObject doc = (BSONObject) JSON.parse(new String(response));
				if (doc instanceof BasicBSONList) {
					record = ((BasicBSONList) doc).toMap().size();
				} else if ((doc instanceof BSONObject) && (((BSONObject) doc)).containsField("meta")
						&& (((BSONObject) doc)).containsField("data")) {
					record = ((BasicBSONList) (((BSONObject) doc)).get("data")).toMap().size();
				}
				
			} catch (Exception ex) {
				ex.printStackTrace();
				record = 1;
			}

			System.out.println(total);
			System.out.println(record);
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			if (in != null) {
				in.close();
			}

			if (br != null) {
				br.close();
			}

			if (out != null) {
				out.close();
			}

		}
		return record;
	}
	


}
