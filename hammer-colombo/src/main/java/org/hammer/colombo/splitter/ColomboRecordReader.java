package org.hammer.colombo.splitter;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.util.HttpURLConnection;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.hammer.colombo.utils.JSON;
import org.hammer.colombo.utils.SocrataUtils;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

/**
 * CKAN record reader
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Colombo
 *
 */
public class ColomboRecordReader extends RecordReader<Object, BSONObject> {

	private static final Log LOG = LogFactory.getLog(ColomboRecordReader.class);

	protected BSONObject current;
	protected final DataSetSplit split;
	protected float seen = 0;
	protected float total = 0.0f;
	protected int socrataRecordLimit = 0;
	protected Configuration conf = null;

	public ColomboRecordReader(final DataSetSplit split) {
		this.split = split;
	}

	@Override
	public void close() {

	}

	@Override
	public Object getCurrentKey() {
		return split.getName();
	}

	@Override
	public BSONObject getCurrentValue() {

		return current;
	}

	@Override
	public void initialize(final InputSplit split, final TaskAttemptContext context) {
		LOG.info("COLOMBO RECORD READER: Get data set");
		socrataRecordLimit = Integer.parseInt(context.getConfiguration().get("socrata.record.limit"));
		this.conf = context.getConfiguration();

		if (conf.get("search-mode").equals("download")) {
			this.current = download();
		} else {
			this.current = search();
		}
		this.seen = 0;
		this.total = 1;

	}

	@Override
	public boolean nextKeyValue() {

		if (seen < total) {
			LOG.info("Read " + (seen + 1) + " documents from (total " + total + ") :");
			LOG.info(" ----> " + this.getCurrentKey().toString());

			seen++;
			return true;
		}

		return false;
	}

	/**
	 * Download data
	 */
	private BSONObject download() {
		BasicBSONObject doc = new BasicBSONObject();
		doc.put("url", split.getUrl());
		doc.put("id", split.getName());

		try {
			LOG.info(split.getUrl());
			try {
				long size = -1;
				LOG.info("--------" + conf.get("simulate") + "-----");
				if (conf.get("simulate").equals("true")) {
					LOG.info("-------- Start simulate -----");
					size = tryGetFileSize(new URL(split.getUrl()));
				} else {
					size = saveUrl(split.getName(), split.getUrl());
				}
				
				doc.put("size", size);
			} catch (Exception e) {
				LOG.error(e);
			}

		} catch (Exception e) {
			LOG.error(e);
		}

		return doc;

	}

	/**
	 * Searcg data
	 */
	private BSONObject search() {
		BSONObject doc = new BasicBSONList();

		try {
			LOG.info(split.getUrl());
			if (split.getDataSetType().equals("org.hammer.santamaria.mapper.dataset.SocrataDataSetInput")) {

				/*
				 * final BasicBSONList list = new BasicBSONList(); int count =
				 * SocrataUtils.CountPackageList(this.conf, split.getUrl(),
				 * split.getName()); int offset = 0; count = (count >
				 * socrataRecordLimit) ? socrataRecordLimit : count;
				 * 
				 * while (offset < count) { BasicBSONList temp = (BasicBSONList)
				 * SocrataUtils.GetDataSet(this.conf, split.getName(),
				 * split.getUrl(), offset, 1000); list.addAll(temp); offset =
				 * offset + 1000; }
				 */
				doc = (BasicBSONList) SocrataUtils.GetDataSet(this.conf, split.getName(), split.getUrl());

			} else {
				HttpClient client = new HttpClient();
				GetMethod method = null;

				try {
					method = new GetMethod(split.getUrl());

					method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
							new DefaultHttpMethodRetryHandler(3, false));
					method.setRequestHeader("User-Agent", "Hammer Project - Colombo query");
					method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Colombo query");

					int statusCode = client.executeMethod(method);

					if (statusCode != HttpStatus.SC_OK) {
						throw new Exception("Method failed: " + method.getStatusLine());
					}
					byte[] responseBody = method.getResponseBody();
					LOG.debug(new String(responseBody));

					if (split.getType().equals("JSON")) {
						doc = (BSONObject) JSON.parse(new String(responseBody));
					} else if (split.getType().equals("CSV")) {
						List<Map<?, ?>> dataMap = readObjectsFromCsv(new String(responseBody));
						String json = returnAsJson(dataMap);
						doc = (BSONObject) JSON.parse(json);
					} else if (split.getType().equals("XML")) {
						throw new Exception("Hammer Colombo datatype not know " + split.getType());
					} else {
						throw new Exception("Hammer Colombo datatype not know " + split.getType());
					}

				} catch (Exception e) {
					LOG.error(e);
				} finally {
					method.releaseConnection();
				}

			}

		} catch (Exception e) {
			LOG.error(e);
		}

		return doc;

	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (seen / total);
	}

	/**
	 * Read object from CSV
	 * 
	 * @param csv
	 * @return
	 * @throws IOException
	 */
	private List<Map<?, ?>> readObjectsFromCsv(String csv) throws IOException {
		MappingIterator<Map<?, ?>> mappingIterator;
		try {
			CsvSchema bootstrap = CsvSchema.emptySchema().withHeader().withColumnSeparator(',');
			CsvMapper csvMapper = new CsvMapper();
			mappingIterator = csvMapper.reader(Map.class).with(bootstrap).readValues(csv);
		} catch (Exception ex) {
			CsvSchema bootstrap = CsvSchema.emptySchema().withHeader().withColumnSeparator(';');
			CsvMapper csvMapper = new CsvMapper();
			mappingIterator = csvMapper.reader(Map.class).with(bootstrap).readValues(csv);
		}
		return mappingIterator.readAll();

	}

	/**
	 * Return data as JSON
	 * 
	 * @param data
	 * @return
	 * @throws IOException
	 */
	private String returnAsJson(List<Map<?, ?>> data) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsString(data);
	}

	/**
	 * Save a file to HDFS
	 * 
	 * @param filename
	 * @param urlString
	 * @return
	 * @throws Exception
	 */
	private long saveUrl(final String filename, final String urlString) throws Exception {
		BufferedWriter br = null;
		InputStream in = null;
		FileSystem hdfs = null;
		OutputStream out = null;
		try {
			hdfs = FileSystem.get(new URI(conf.get("hdfs-site")), conf);
			Path file = new Path(conf.get("download") + "/" + filename);
			if (hdfs.exists(file)) {
				hdfs.delete(file, true);
			}
			out = hdfs.create(file);
			br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
			in = new BufferedInputStream(new URL(urlString).openStream());

			long total = IOUtils.copyLarge(in, out);
			
			return total;
		} catch (Exception e) {
			LOG.error(e);
			throw e;
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
			
			if (hdfs != null) {
				hdfs.close();
			}
			
			

		}
	}
	
	/**
	 * Get file Size
	 * @param url
	 * @return
	 */
	private long tryGetFileSize(URL url) {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("HEAD");
            conn.getInputStream();
            return conn.getContentLength();
        } catch (IOException e) {
            return -1;
        } finally {
            conn.disconnect();
        }
    }
}
