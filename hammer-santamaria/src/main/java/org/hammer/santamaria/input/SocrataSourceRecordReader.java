package org.hammer.santamaria.input;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.hammer.santamaria.splitter.BaseDataSourceRecordReader;
import org.hammer.santamaria.splitter.DataSourceSplit;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;

/**
 * Socrata record reader
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class SocrataSourceRecordReader extends BaseDataSourceRecordReader {

	private static final Log LOG = LogFactory.getLog(SocrataSourceRecordReader.class);

	private static final String COUNT = "?$select=count(*)";

	/**
	 * Output from Socrata source
	 */
	private String output = "";

	/**
	 * Data set
	 */
	private ArrayList<BSONObject> dataset = new ArrayList<BSONObject>();

	public SocrataSourceRecordReader(final DataSourceSplit split) {
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
		LOG.info("SANTA MARIA RECORD READER: Get package list from SOCRATA site");
		this.dataset = new ArrayList<BSONObject>();
		this.countPackageList();
		int offset = 0;
		while (offset < total) {
			this.getPackageList(offset, 500);
			offset = offset + 500;
		}
	}

	@Override
	public boolean nextKeyValue() {

		if (seen < total) {
			LOG.info("Read " + (seen + 1) + " documents from (total " + total + ") :");
			LOG.info(split.getName() + " ----- " + this.dataset.get((int) seen));

			this.current = new BasicBSONObject();
			this.current.put("datasource", split.getName());
			this.current.put("dataset", this.dataset.get((int) seen).get("id"));
			this.current.put("dataset-obj", this.dataset.get((int) seen));
			this.current.put("datainput_type", "org.hammer.santamaria.mapper.dataset.SocrataDataSetInput");
			this.current.put("url", split.getUrl());

			seen++;
			return true;
		}

		return false;
	}
	

	/**
	 * Encode URI
	 * 
	 * @param s
	 * @return
	 */
	public String encodeURIComponent(String s) {
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
	 * Get data set from CKAN repository
	 */
	private void getPackageList(int offset, int limit) {
		HttpClient client = new HttpClient();
		LOG.info(split.getUrl() + "?$offset=" + offset + "&$limit=" + limit);
		GetMethod method = new GetMethod(split.getUrl() + "?$offset=" + offset + "&$limit=" + limit + "&$where="  + encodeURIComponent("tipo='Dataset'"));
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

			@SuppressWarnings("unchecked")
			ArrayList<BasicDBObject> docs = (ArrayList<BasicDBObject>) JSON.parse(new String(responseBody));

			for (BasicDBObject doc : docs) {
				BSONObject temp = new BasicBSONObject();
				if (doc.containsField("tipo") && doc.getString("tipo").equals("Dataset")) {
					temp.put("id", doc.getString("identificatore"));
					temp.put("datasource", split.getName());
					temp.put("datainput_type", "org.hammer.santamaria.mapper.dataset.SocrataDataSetInput");
					temp.put("name", doc.getString("nome"));
					temp.put("title", doc.getString("descrizione"));
					temp.put("author", doc.getString("display_owner"));
					temp.put("author_email", doc.getString("mail_contatto"));
					temp.put("tipo", doc.getString("tipo"));
					temp.put("dataset-type", "JSON");
					temp.put("url", ((BasicDBObject) doc.get("link_api")).getString("url"));
					temp.put("created", doc.get("data_creazione"));
					temp.put("description", doc.get("descrizione"));
					temp.put("revision_timestamp", doc.get("data_ultimo_aggiornamento"));
					ArrayList<String> tags = new ArrayList<String>();

					if (doc.keySet().contains("keywords")) {
						StringTokenizer st = new StringTokenizer(doc.getString("keywords"), ",");
						while (st.hasMoreElements()) {
							tags.add(st.nextElement().toString().trim().toLowerCase());
						}
					}
					temp.put("tags", tags);

					dataset.add(temp);
				}

			}

			LOG.info("SANTA MARIA SOCRATA RECORD READER found" + this.dataset.size());

		} catch (Exception e) {
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}
	}

	/**
	 * Count record from Socrata
	 */
	private void countPackageList() {
		this.total = 0;
		this.seen = 0;
		HttpClient client = new HttpClient();
		LOG.info(split.getUrl());
		GetMethod method = new GetMethod(split.getUrl() + COUNT + "&$where="  + encodeURIComponent("tipo='Dataset'"));
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

			@SuppressWarnings("unchecked")
			ArrayList<BasicDBObject> docs = (ArrayList<BasicDBObject>) JSON.parse(new String(responseBody));
			for (BasicDBObject doc : docs) {
				if (doc.keySet().contains("count")) {
					this.total = Integer.parseInt(doc.getString("count"));
				}

			}

			LOG.info("SANTA MARIA SOCRATA RECORD READER count " + this.total);
		} catch (Exception e) {
			LOG.error(e);
			e.printStackTrace();
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

	public ArrayList<BSONObject> getDataset() {
		return dataset;
	}

	public void setDataset(ArrayList<BSONObject> dataset) {
		this.dataset = dataset;
	}

}
