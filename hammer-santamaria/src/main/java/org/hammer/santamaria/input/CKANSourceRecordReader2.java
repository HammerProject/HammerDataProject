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
 * CKAN record reader (implement limit / count
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class CKANSourceRecordReader2 extends BaseDataSourceRecordReader {

	private static final Log LOG = LogFactory.getLog(CKANSourceRecordReader2.class);

	private static final String PACKAGE_LIST_COMMAND = "/package_list";
	/**
	 * Output from CKAN source
	 */
	private String output = "";

	/**
	 * Data set
	 */
	private ArrayList<String> dataset = new ArrayList<String>();

	public CKANSourceRecordReader2(final DataSourceSplit split) {
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
		LOG.info("SANTA MARIA RECORD READER: Get package list from CKAN site");
		this.total = 0;
		this.seen = 0;
		int error = 0;
		int offset = 0;
		int limit = 1000;
		this.dataset = new ArrayList<String>();
		while(error == 0) {
			try {
				this.getPackageList(offset, limit);
				offset += limit;
				limit += limit;
			} catch(Exception ex) {
				error = 1;
			}
		}
	}

	@Override
	public boolean nextKeyValue() {

		if (seen < total) {
			LOG.info("Read " + (seen + 1) + " documents from (total " + total + ") :");
			LOG.debug(split.getName() + " ----- " + this.dataset.get((int) seen));

			this.current = new BasicBSONObject();
			this.current.put("datasource", split.getName());
			this.current.put("dataset", this.dataset.get((int) seen));
			// TODO IMPLEMENTARE ALTRE VERSIONI DI CKAN

			this.current.put("datainput_type", "org.hammer.santamaria.mapper.dataset.CKANDataSetInput");
			this.current.put("url", split.getUrl());

			seen++;
			return true;
		}

		return false;
	}

	/**
	 * Get data set from CKAN repository
	 */
	@SuppressWarnings("unchecked")
	private void getPackageList(int offset, int limit) throws Exception {
		HttpClient client = new HttpClient();
		LOG.info(split.getUrl() + PACKAGE_LIST_COMMAND + "?offset=" + offset + "&limit=" + limit);
		GetMethod method = new GetMethod(split.getUrl() + PACKAGE_LIST_COMMAND + "?offset=" + offset + "&limit=" + limit);
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
				
		method.setRequestHeader("User-Agent", "Hammer Project - SantaMaria crawler");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - SantaMaria crawler");
		
		try {
			int statusCode = client.executeMethod(method);
			
			if (statusCode != HttpStatus.SC_OK) {
				LOG.info("Ops. Error " + statusCode + " try with " + split.getUrl() + "/package_search");
				method = new GetMethod(split.getUrl() + "/package_search");
				method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
				statusCode = client.executeMethod(method);
			}
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			LOG.debug(new String(responseBody));
			setOutput(new String(responseBody));

			Document doc = Document.parse(getOutput());

			/*
			 * TODO CHECK VERSIONE CKAN
			 * 
			 * 
			 */
			if (doc.containsKey("result")) {
				this.dataset.addAll((ArrayList<String>) doc.get("result")) ;
				for (String k : this.dataset) {
					LOG.debug("Document: " + k);
				}
				LOG.info("SANTA MARIA CKAN RECORD READER found" + this.dataset.size());
				this.total += this.dataset.size();
				if(((ArrayList<String>) doc.get("result")).size() != limit) {
					throw new Exception(((ArrayList<String>) doc.get("result")).size() + " != " + limit);
				}
			}
		} catch (Exception e) {
			LOG.error(e);
			throw e;
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
}
