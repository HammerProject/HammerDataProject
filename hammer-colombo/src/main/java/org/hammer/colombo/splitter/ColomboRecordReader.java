package org.hammer.colombo.splitter;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.client.params.ClientPNames;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.types.BasicBSONList;
import org.hammer.colombo.utils.JSON;
import org.hammer.colombo.utils.SocrataUtils;
import org.hammer.isabella.cc.util.IsabellaUtils;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.mongodb.BasicDBList;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Colombo record reader
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
		doc.put("action", split.getAction());
		doc.put("dataset", split.getDataset());


		try {
			LOG.info(split.getUrl());
			try {
				long size = -1;
				size = saveUrl(split.getName(), split.getUrl());
				if (split.getDataSetType().equals("org.hammer.santamaria.mapper.dataset.SocrataDataSetInput")) {
					doc.put("record-selected", SocrataUtils.CountPackageList(conf, split.getUrl(), split.getName()));
					doc.put("record-total", SocrataUtils.CountTotalPackageList(split.getUrl(), split.getName()));
				} else if (split.getDataSetType().equals("org.hammer.santamaria.mapper.dataset.Socrata2DataSetInput")) {
					doc.put("selectedRecord", SocrataUtils.CountPackageList(conf, split.getUrl(), split.getName()));
					doc.put("record-total", SocrataUtils.CountTotalPackageList(split.getUrl(), split.getName()));
				} else {
					doc.put("record-total", countRecord(split.getName()));
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
				 * SocrataUtils.CountPackageList(this.conf, split.getUrl(), split.getName());
				 * int offset = 0; count = (count > socrataRecordLimit) ? socrataRecordLimit :
				 * count;
				 * 
				 * while (offset < count) { BasicBSONList temp = (BasicBSONList)
				 * SocrataUtils.GetDataSet(this.conf, split.getName(), split.getUrl(), offset,
				 * 1000); list.addAll(temp); offset = offset + 1000; }
				 */
				doc = (BasicBSONList) SocrataUtils.GetDataSet(this.conf, split.getName(), split.getUrl());

			} else if (split.getDataSetType().equals("org.hammer.santamaria.mapper.dataset.MongoDBDataSetInput")) {
				MongoClientURI connectionString = new MongoClientURI(split.getUrl());
				MongoClient mongoClient = new MongoClient(connectionString);
				MongoDatabase db = mongoClient.getDatabase(connectionString.getDatabase());
				BasicBSONList newList = new BasicBSONList();
				try {
					MongoCollection<Document> collection = db.getCollection(split.getDatasource());
					FindIterable<Document> record = collection.find();
					for (Document d : record) {
						BSONObject newObj = (BSONObject) JSON.parse(new String(d.toJson()));
						newList.add(newObj);
					}

					doc = newList;

				} catch (Exception ex) {

				} finally {
					mongoClient.close();
				}

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

						// check if is a view + data object
						ArrayList<String> meta = new ArrayList<String>();
						BasicBSONList pList = null;

						for (String metaKey : doc.keySet()) {
							if (!meta.contains(metaKey.toLowerCase())) {
								meta.add(metaKey.toLowerCase());
							}
						}

						if ((meta.size() == 2) && (meta.contains("meta")) && (meta.contains("data"))) {
							pList = (BasicBSONList) ((BSONObject) ((BSONObject) doc.get("meta")).get("view"))
									.get("columns");
							meta = new ArrayList<String>();
							for (Object obj : pList) {
								BasicBSONObject pObj = (BasicBSONObject) obj;
								if (pObj.containsField("fieldName")) {
									meta.add(pObj.getString("fieldName").toLowerCase().replaceAll(":", ""));
								}
							}

							BasicBSONList newList = new BasicBSONList();
							pList = (BasicBSONList) doc.get("data");
							for (Object obj : pList) {
								BasicDBList bObj = (BasicDBList) obj;
								BSONObject newObj = GetDataByItem(meta, bObj);
								newList.add(newObj);
							}
							doc = newList;
						}

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
	 * Count record
	 * 
	 * @return
	 */
	private long countRecord(String file) {
		long record = 0;
		try {
			String response = ReadFileFromHdfs(conf, file);
			LOG.info(response);
			BSONObject doc = (BSONObject) JSON.parse(new String(response));

			if (doc instanceof BasicBSONList) {
				record = ((BasicBSONList) doc).toMap().size();
			} else if ((doc instanceof BSONObject) && (((BSONObject) doc)).containsField("meta")
					&& (((BSONObject) doc)).containsField("data")) {
				record = ((BasicBSONList) (((BSONObject) doc)).get("data")).toMap().size();
			}
		} catch (Exception ex) {
			LOG.error(ex);
			record = 1;
		}
		return record;
	}

	/**
	 * Get file Size
	 * 
	 * @param url
	 * @return
	 */
	protected long tryGetFileSize(String url) {

		HttpURLConnection conn = null;
		try {
			URL wUrl = new URL(url);
			conn = (HttpURLConnection) wUrl.openConnection();
			conn.setRequestMethod("HEAD");
			conn.getInputStream();
			return conn.getContentLength();
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			conn.disconnect();
		}

		HttpClient client = new HttpClient();
		client.getHttpConnectionManager().getParams().setParameter(ClientPNames.HANDLE_REDIRECTS, false);
		GetMethod method = new GetMethod(url);
		client.getHttpConnectionManager().getParams().setConnectionTimeout(3000);
		client.getHttpConnectionManager().getParams().setSoTimeout(2000);
		method.setRequestHeader("User-Agent", "Hammer Project - Colombo");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Colombo");
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

		try {
			int statusCode = client.executeMethod(method);
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			long l = method.getResponseContentLength();
			if (l == -1) {
				l = method.getResponseBody().length;
			}
			return l;

		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e);
			return -1;
		} finally {
			method.releaseConnection();
		}
	}

	public static String ReadFileFromHdfs(Configuration conf, String filename) {
		FileSystem fs = null;
		BufferedReader br = null;
		StringBuilder sb = null;
		try {
			Path pt = new Path(conf.get("download") + "/" + filename);
			fs = FileSystem.get(new Configuration());
			br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			sb = new StringBuilder();
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (Exception e) {
				}
			}
			if (br != null) {
				try {
					br.close();
				} catch (Exception e) {
				}
			}

		}

		return sb.toString();
	}

	//
	public static void main(String[] pArgs) throws Exception {
		InputStream in = null;
		BufferedWriter br = null;
		OutputStream out = null;
		try {
			out = new FileOutputStream("test.json");
			br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
			in = new BufferedInputStream(new URL("https://data.cityofnewyork.us/resource/kpav-sd4t.json").openStream());

			long record = 0;
			long total = IOUtils.copyLarge(in, out);
			String response = IsabellaUtils.readFile("test.json");
			try {
				BSONObject doc = (BSONObject) JSON.parse(new String(response));
				if (doc instanceof BasicBSONList) {
					record = ((BasicBSONList) doc).toMap().size();
				} else if ((doc instanceof BSONObject) && (((BSONObject) doc)).containsField("meta")
						&& (((BSONObject) doc)).containsField("data")) {
					record = ((BasicBSONList) (((BSONObject) doc)).get("data")).toMap().size();
				}
				record = SocrataUtils.CountTotalPackageList("https://data.cityofnewyork.us/resource/kpav-sd4t.json",
						"kpav-sd4t");
			} catch (Exception ex) {
				ex.printStackTrace();
				record = 1;
			}

			System.out.println(total);
			System.out.println(record);
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

		}
	}

	/**
	 * Get data from bson obj not in key-value format
	 * 
	 * @param bObj
	 * @return
	 */
	public static BSONObject GetDataByItem(ArrayList<String> meta, BasicDBList bObj) {
		BSONObject newObj = new BasicBSONObject();
		int i = 0;
		for (String t : meta) {
			LOG.debug(t + " --- ");
			newObj.put(t, bObj.get(i));
			i++;
		}
		return newObj;
	}
}
