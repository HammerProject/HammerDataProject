package org.hammer.santamaria.input;

import java.util.ArrayList;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.hammer.santamaria.splitter.BaseDataSourceRecordReader;
import org.hammer.santamaria.splitter.DataSourceSplit;

/**
 * CKAN record reader (4 big source)
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class CKAN3BigSourceRecordReader extends BaseDataSourceRecordReader {

	public static final int LIMIT = 500;
	
	public static final String ACTION = "/package_search?start=";
	
	private static final Log LOG = LogFactory.getLog(CKAN3BigSourceRecordReader.class);

	/**
	 * Output from CKAN source
	 */
	private String output = "";

	/**
	 * Data set
	 */
	private ArrayList<String> dataset = new ArrayList<String>();

	public CKAN3BigSourceRecordReader(final DataSourceSplit split) {
		super(split);
	}

	@Override
	public void close() {

	}

	@Override
	public Object getCurrentKey() {
		if (dataset.size() > 0 && seen < total) {
			return this.dataset.get((int) seen);
		}
		return NullWritable.get();
	}

	@Override
	public BSONObject getCurrentValue() {
		return current;
	}

	@Override
	public void initialize(final InputSplit split, final TaskAttemptContext context) {
		LOG.info("SANTA MARIA RECORD READER: Get package list from CKAN site (BIG SOURCE Version)");
		this.total = 0;
		this.seen = 0;
		this.dataset = new ArrayList<String>();
		this.getPackageList();
		this.total = this.dataset.size();
	}

	@Override
	public boolean nextKeyValue() {

		if (seen < total) {
			LOG.info("Read " + (seen + 1) + " documents from (total " + total + ") :");
			LOG.debug(split.getName() + " ----- " + this.dataset.get((int) seen));

			this.current = new BasicBSONObject();
			this.current.put("datasource", split.getName());
			this.current.put("dataset", this.dataset.get((int) seen));

			this.current.put("datainput_type", "org.hammer.santamaria.mapper.dataset.CKANDataSetInput");
			this.current.put("url", split.getUrl());
			this.current.put("action", split.getAction());

			seen++;
			return true;
		}

		return false;
	}

	/**
	 * Get data set from CKAN repository
	 * 
	 * 4 Big Source --> Direct Link
	 */
	@SuppressWarnings("unchecked")
	private void getPackageList() {
		HttpClient client = new HttpClient();
		LOG.info(split.getAction());
		GetMethod method = new GetMethod(split.getAction());
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
				
		method.setRequestHeader("User-Agent", "Hammer Project - SantaMaria crawler");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - SantaMaria crawler");
		
		try {
			int statusCode = client.executeMethod(method);
			
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			LOG.debug(new String(responseBody));
			setOutput(new String(responseBody));

			Document document = Document.parse(getOutput());

			if (document.containsKey("result")) {
				
				ArrayList<Document> docs = (ArrayList<Document>) ((Document) document.get("result")).get("results");

				for (Document doc : docs) {
					this.dataset.add(doc.getString("id"));
				}

				LOG.info("SANTA MARIA CKAN3 RECORD READER found" + this.dataset.size());
			}
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}
	}

	/**
	 * @return the output
	 */
	public String getOutput() {
		return output;
	}

	/**
	 * @param output
	 *            the output to set
	 */
	public void setOutput(String output) {
		this.output = output;
	}

	public ArrayList<String> getDataset() {
		return dataset;
	}

	public void setDataset(ArrayList<String> dataset) {
		this.dataset = dataset;
	}
	
	
	public static int GetCountByCkan3(String url) {
		int count = 0;
		HttpClient client = new HttpClient();
		LOG.info("**** INPUT SPLIT COUNT *** " + url);
		GetMethod method = new GetMethod(url);
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
				
		method.setRequestHeader("User-Agent", "Hammer Project - SantaMaria crawler");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - SantaMaria crawler");
		
		try {
			client.executeMethod(method);
			byte[] responseBody = method.getResponseBody();
			Document doc = Document.parse(new String(responseBody));
			if (doc.containsKey("result")) {
				count = ((Document)doc.get("result")).getInteger("count");
				LOG.info("Find --> " + count);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}
		return count;
	}
	
	/**
	 * Test
	 * 
	 * @param pArgs
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] pArgs) throws Exception {
		GetCountByCkan3("http://catalog.data.gov/api/action/package_search?start=0&rows=1");
		
		
		HttpClient client = new HttpClient();
		GetMethod method = new GetMethod("http://catalog.data.gov/api/action/package_search?start=0&rows=10");
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
				
		method.setRequestHeader("User-Agent", "Hammer Project - SantaMaria crawler");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - SantaMaria crawler");
		
		try {
			int statusCode = client.executeMethod(method);
			
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			LOG.info(new String(responseBody));

			Document document = Document.parse(new String(responseBody));

			if (document.containsKey("result")) {
				
				ArrayList<Document> docs = (ArrayList<Document>) ((Document) document.get("result")).get("results");

				for (Document doc : docs) {
					LOG.info(doc.getString("id"));
				}

			}
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}
	}
}
