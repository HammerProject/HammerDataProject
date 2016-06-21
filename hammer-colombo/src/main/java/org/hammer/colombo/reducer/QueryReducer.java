package org.hammer.colombo.reducer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.hammer.colombo.splitter.ColomboRecordReader;
import org.hammer.colombo.utils.JSON;
import org.hammer.colombo.utils.StatUtils;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.cc.util.IsabellaUtils;
import org.hammer.isabella.fuzzy.JaroWinkler;
import org.hammer.isabella.query.Edge;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Keyword;
import org.hammer.isabella.query.Node;
import org.hammer.isabella.query.QueryGraph;
import org.hammer.isabella.query.ValueNode;

import com.mongodb.BasicDBList;
import com.mongodb.hadoop.io.BSONWritable;

/**
 * Query reducer
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Colombo
 *
 */
public class QueryReducer extends Reducer<Text, BSONWritable, Text, BSONWritable> {

	public static final Log LOG = LogFactory.getLog(QueryReducer.class);

	private Configuration conf = null;
	private QueryGraph q = null;
	private float thSim = 0.0f;

	@Override
	protected void setup(Reducer<Text, BSONWritable, Text, BSONWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		LOG.info("SETUP REDUCE2 - Hammer Colombo Project");
		this.conf = context.getConfiguration();
		final HashMap<String, Keyword> kwIndex = StatUtils.GetMyIndex(conf);
		thSim = Float.parseFloat(conf.get("thSim"));
		Isabella parser = new Isabella(new StringReader(conf.get("query-string")));
		try {
			q = parser.queryGraph();
			q.setIndex(kwIndex);
		} catch (ParseException e) {
			throw new InterruptedException(e.getMessage());
		}
		for (IsabellaError err : parser.getErrors().values()) {
			LOG.error(err.toString());
		}

	}

	@Override
	public void reduce(final Text pKey, final Iterable<BSONWritable> pValues, final Context pContext)
			throws IOException, InterruptedException {
		Configuration conf = pContext.getConfiguration();

		LOG.debug("START COLOMBO REDUCER2");

		if (conf.get("search-mode").equals("download")) {

			long size = 0;
			long record = 0;
			long selectedRecord = 0;
			long count = 0;
			for (final BSONWritable value : pValues) {
				size += (value.getDoc().containsField("size")) ? (Long) value.getDoc().get("size") : 0;
				record += (value.getDoc().containsField("record-total")) ? (Long) value.getDoc().get("record-total")
						: 0;
				selectedRecord += (value.getDoc().containsField("record-selected"))
						? (Long) value.getDoc().get("record-selected") : 0;

				count++;
			}
			

			// save the stat
			BSONObject statObj = new BasicBSONObject();
			statObj.put("type", "stat");
			statObj.put("record-total", record);
			statObj.put("record-selected", selectedRecord);
			statObj.put("resource-count", count);
			statObj.put("size", size);
			statObj.put("fuzzy-query", 0);
			StatUtils.SaveStat(this.conf, statObj);

			// download output doesn't sent record to commiter and record writer
		} else {
			//
			// key = column-value --> pKey
			// value = the record --> pValues
			// so
			// if key match the where condition we take the record
			// else we doesn't store the record
			LOG.info("---------------------------------------------------");
			StringTokenizer st = new StringTokenizer(pKey.toString(), "|");
			String column = st.nextToken().toLowerCase();
			String value = st.nextToken().toLowerCase();

			boolean c = false;
			for (Edge en : q.getQueryCondition()) {
				LOG.info(en.getCondition());
				LOG.info(en.getOperator());
				LOG.info("------------------------------------");
				for (Node ch : en.getChild()) {
					LOG.info(en.getName().toLowerCase() + " -- " + column.toLowerCase());
					LOG.info(ch.getName().toLowerCase() + " -- " + value);

					if ((ch instanceof ValueNode) && en.getCondition().equals("or")) {

						double sim = JaroWinkler.Apply(en.getName().toLowerCase(), column.toLowerCase());
						LOG.info("ok" + sim);
						if (sim > thSim) {
							LOG.info("ok" + sim);
							if (en.getOperator().equals("eq") && ch.getName().toLowerCase().equals(value)) {
								c = true;
							} else if (en.getOperator().equals("gt")) {
								if (ch.getName().toLowerCase().compareTo(value) > 0) {
									c = true;
								}
							} else if (en.getOperator().equals("lt")) {
								if (ch.getName().toLowerCase().compareTo(value) < 0) {
									c = true;
								}
							} else if (en.getOperator().equals("ge")) {
								if (ch.getName().toLowerCase().compareTo(value) >= 0) {
									c = true;
								}
							} else if (en.getOperator().equals("le")) {
								if (ch.getName().toLowerCase().compareTo(value) <= 0) {
									c = true;
								}
							} else if (en.getName().equals(column)) {
								if (ch.getName().toLowerCase().compareTo(value) == 0) {
									c = true;
								}
							}
						}
					}
				}
			}

			LOG.info("---> " + c);
			long size = 0;
			long totalRecord = 0;
			long selectedRecord = 0;

			for (final BSONWritable record : pValues) {
				if (c) {
					pContext.write(new Text(record.hashCode() + ""), record);
					selectedRecord++;
					size += record.getDoc().toString().length();
				}
				totalRecord++;
			}

			// save the stat
			BSONObject statObj = new BasicBSONObject();
			statObj.put("type", "stat");
			statObj.put("record-total", totalRecord);
			statObj.put("record-selected", selectedRecord);
			statObj.put("resource-count", 0);
			statObj.put("size", size);
			statObj.put("fuzzy-query", 0);
			StatUtils.SaveStat(this.conf, statObj);

			LOG.debug("COLOMBO REDUCE2 - FOUND AND WRITE " + pKey + " DATASET ");
		}

	}

	//
	// test function
	//
	public static void main(String[] pArgs) throws Exception {
		HttpClient client = new HttpClient();
		GetMethod method = null;

		try {
			method = new GetMethod("https://www.opendata.go.ke/api/views/2faz-jghi/rows.json?accessType=DOWNLOAD");

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

			BSONObject doc = (BSONObject) JSON.parse(new String(responseBody));

			// check if is a view + data object
			ArrayList<String> meta = new ArrayList<String>();
			BasicBSONList pList = null;

			for (String metaKey : doc.keySet()) {
				if (!meta.contains(metaKey.toLowerCase())) {
					meta.add(metaKey.toLowerCase());
				}
			}

			if ((meta.size() == 2) && (meta.contains("meta")) && (meta.contains("data"))) {
				pList = (BasicBSONList) ((BSONObject) ((BSONObject) doc.get("meta")).get("view")).get("columns");
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
					BSONObject newObj = ColomboRecordReader.GetDataByItem(meta, bObj);
					newList.add(newObj);
				}
				doc = newList;

			}

			ArrayList<String> t = new ArrayList<String>();
			ArrayList<String> r = new ArrayList<String>();

			if (doc instanceof BasicBSONList) {

				BasicBSONList newList = (BasicBSONList) doc;
				// for each record we store a key-value
				// - key = column-value
				// - value = the record
				for (Object pObj : newList) {

					if (pObj instanceof BSONObject) {
						BSONObject bsonObj = (BSONObject) pObj;
						for (Object column : bsonObj.keySet()) {
							String columnName = (String) column;
							LOG.debug(columnName + "|" + bsonObj.get(columnName));
							t.add(columnName + "|" + bsonObj.get(columnName));
						}
					}
				}
			}

			String filePath = "example_1f.json";
			Isabella parser = new Isabella(new StringReader(IsabellaUtils.readFile(filePath)));
			QueryGraph q;
			try {
				q = parser.queryGraph();
			} catch (ParseException e) {
				throw new InterruptedException(e.getMessage());
			}
			for (IsabellaError err : parser.getErrors().values()) {
				LOG.error(err.toString());
			}
			float thSim = 0.9f;

			for (String pKey : t) {
				LOG.info("---------------------------------------------------");
				LOG.info(pKey);
				StringTokenizer st = new StringTokenizer(pKey.toString(), "|");
				String column = st.nextToken().toLowerCase();
				String value = st.nextToken().toLowerCase();

				boolean c = false;
				for (Edge en : q.getQueryCondition()) {
					LOG.info(en.getCondition());
					LOG.info(en.getOperator());
					LOG.info("------------------------------------");
					for (Node ch : en.getChild()) {
						LOG.info(en.getName().toLowerCase() + " -- " + column.toLowerCase());
						LOG.info(ch.getName().toLowerCase() + " -- " + value);

						if ((ch instanceof ValueNode) && en.getCondition().equals("or")) {

							double sim = JaroWinkler.Apply(en.getName().toLowerCase(), column.toLowerCase());
							LOG.info("ok" + sim);
							if (sim > thSim) {
								LOG.info("ok" + sim);
								if (en.getOperator().equals("eq") && ch.getName().toLowerCase().equals(value)) {
									c = true;
								} else if (en.getOperator().equals("gt")) {
									if (ch.getName().toLowerCase().compareTo(value) > 0) {
										c = true;
									}
								} else if (en.getOperator().equals("lt")) {
									if (ch.getName().toLowerCase().compareTo(value) < 0) {
										c = true;
									}
								} else if (en.getOperator().equals("ge")) {
									if (ch.getName().toLowerCase().compareTo(value) >= 0) {
										c = true;
									}
								} else if (en.getOperator().equals("le")) {
									if (ch.getName().toLowerCase().compareTo(value) <= 0) {
										c = true;
									}
								} else if (en.getName().equals(column)) {
									if (ch.getName().toLowerCase().compareTo(value) == 0) {
										c = true;
									}
								}
							}
						}
					}
				}

				LOG.info("---> " + c);
				if (c) {
					r.add(pKey);
				}
			}

			for (String result : r) {
				System.out.println(result);
			}

		} catch (Exception e) {
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}

	}
}