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
 * CKAN record reader (4 big source - v3 of CKAN)
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class CKANBigSourceRecordReader extends BaseDataSourceRecordReader {

	public static final int LIMIT = 100;
	
	public static final String ACTION = "/package_list?offset=";
	
	private static final Log LOG = LogFactory.getLog(CKANBigSourceRecordReader.class);

	/**
	 * Output from CKAN source
	 */
	private String output = "";

	/**
	 * Data set
	 */
	private ArrayList<String> dataset = new ArrayList<String>();

	public CKANBigSourceRecordReader(final DataSourceSplit split) {
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
		LOG.info("SANTA MARIA RECORD READER: Get package list from CKAN3 site (BIG SOURCE Version3)");
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

			Document doc = Document.parse(getOutput());

			if (doc.containsKey("result")) {
				this.dataset.addAll((ArrayList<String>) doc.get("result")) ;
				for (String k : this.dataset) {
					LOG.debug("Document: " + k);
				}
				LOG.info("SANTA MARIA CKAN RECORD READER found" + this.dataset.size());
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
	
	
	@SuppressWarnings("unchecked")
	public static int GetCountByCkan(String url) {
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
				count = ((ArrayList<String>) doc.get("result")).size();
			}
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}
		return count;
	}
}
