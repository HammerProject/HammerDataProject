package org.hammer.colombo.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.util.ArrayList;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.types.BasicBSONList;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.query.Edge;
import org.hammer.isabella.query.Node;
import org.hammer.isabella.query.NumberValueNode;
import org.hammer.isabella.query.QueryGraph;
import org.hammer.isabella.query.TextValueNode;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * Socrata Utils
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Colombo
 *
 */
public class SocrataUtils {

	/**
	 * Encode URI
	 * 
	 * @param s
	 * @return
	 */
	public static String EncodeURIComponent(String s) {
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

	private static final Log LOG = LogFactory.getLog(SocrataUtils.class);

	/**
	 * Test Socrata Utils
	 * 
	 * @param pArgs
	 * @throws Exception
	 */
	public static void main(String[] pArgs) throws Exception {
		final BasicBSONList list = new BasicBSONList();
		int count = 23656;
		int offset = 0;

		while (offset < count) {
			//System.out.println(offset);
			BasicBSONList temp = new BasicBSONList();
			String urlStr = "https://www.dati.lombardia.it/resource/rsg3-xhvk.json?$offset=" + offset
					+ "&$limit=1000&$where=" + EncodeURIComponent("tipologia_combustibile='Gasolio'");
			final GetMethod method = new GetMethod(urlStr);
			final HttpClient client = new HttpClient();
			method.setRequestHeader("User-Agent", "Hammer Project - Colombo");
			method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Colombo");
			method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
					new DefaultHttpMethodRetryHandler(3, false));
			try {
				int statusCode = client.executeMethod(method);
				if (statusCode != HttpStatus.SC_OK) {
					throw new Exception("Method failed: " + method.getStatusLine());
				}
				byte[] responseBody = method.getResponseBody();
				// temp = (BasicBSONList) JSON.parse(new String(responseBody));

				Gson gson = new Gson();
				Type collectionType = new TypeToken<BasicBSONList>() {
				}.getType();
				temp = gson.fromJson(new String(responseBody), collectionType);

				@SuppressWarnings("rawtypes")
				com.google.gson.internal.LinkedTreeMap gObj = (com.google.gson.internal.LinkedTreeMap) temp.get(0);
				BSONObject bObj = new BasicBSONObject();
				for (Object gKey : gObj.keySet()) {
					bObj.put(gKey.toString(), gObj.get(gKey));
				}

				//System.out.print(temp.size());
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				method.releaseConnection();
			}

			list.addAll(temp);
			offset = offset + 1000;
		}
	}

	/**
	 * Get number of record from Socrata Data Set
	 * 
	 * @param url
	 * @return
	 * @throws Exception
	 */
	public static long CountPackageList(Configuration conf, String url, String id) throws Exception {
		HttpClient client = new HttpClient();
		client.getHttpConnectionManager().getParams().setConnectionTimeout(3000);
		client.getHttpConnectionManager().getParams().setSoTimeout(2000);

		String socrataQuery = CreateWhereCondition(conf, id);

		long count = 0;
		String urlStr = url + "?$select=count(*)&$where=" + EncodeURIComponent(socrataQuery);
		//SaveData(conf, id, urlStr, socrataQuery);

		GetMethod method = new GetMethod(urlStr);
		LOG.info("COUNT RECORD BY SOCRATA -> " + urlStr);
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		method.setRequestHeader("User-Agent", "Hammer Project - Colombo");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Colombo");
		method.getParams().setParameter("$select", "count(*)");
		method.getParams().setParameter("$where", socrataQuery);

		try {
			int statusCode = client.executeMethod(method);

			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			//LOG.info(new String(responseBody));
			@SuppressWarnings("unchecked")
			ArrayList<BasicDBObject> docs = (ArrayList<BasicDBObject>) JSON.parse(new String(responseBody));
			for (BasicDBObject doc : docs) {
				if (doc.keySet().contains("count")) {
					count = Integer.parseInt(doc.getString("count"));
				}

			}
			LOG.info("COUNT RECORD BY SOCRATA -> " + count);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		} finally {
			method.releaseConnection();
		}
		return count;
	}

	
	/**
	 * Get number of record from Socrata Data Set
	 * 
	 * @param url
	 * @return
	 * @throws Exception
	 */
	public static long CountTotalPackageList(String url, String id) throws Exception {
		HttpClient client = new HttpClient();
		client.getHttpConnectionManager().getParams().setConnectionTimeout(3000);
		client.getHttpConnectionManager().getParams().setSoTimeout(2000);

		long count = 0;
		String urlStr = url + "?$select=count(*)";

		GetMethod method = new GetMethod(urlStr);
		LOG.info("COUNT RECORD BY SOCRATA -> " + urlStr);
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		method.setRequestHeader("User-Agent", "Hammer Project - Colombo");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Colombo");
		method.getParams().setParameter("$select", "count(*)");

		try {
			int statusCode = client.executeMethod(method);

			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			//LOG.info(new String(responseBody));
			@SuppressWarnings("unchecked")
			ArrayList<BasicDBObject> docs = (ArrayList<BasicDBObject>) JSON.parse(new String(responseBody));
			for (BasicDBObject doc : docs) {
				if (doc.keySet().contains("count")) {
					count = Integer.parseInt(doc.getString("count"));
				}

			}
			LOG.info("COUNT RECORD BY SOCRATA -> " + count);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		} finally {
			method.releaseConnection();
		}
		return count;
	}
	
	/**
	 * Return a dataset from SOCRATA Site
	 * 
	 * @param url
	 * @param datasource
	 * @param id
	 * @param c
	 * @return
	 * @throws Exception
	 */
	public static BasicBSONList GetDataSet(Configuration conf, String id, String url, int offset, int limit)
			throws Exception {
		BasicBSONList pList = new BasicBSONList();
		String socrataQuery = CreateWhereCondition(conf, id);

		String urlStr = url + "?$offset=" + offset + "&$limit=" + limit + "&$where=" + EncodeURIComponent(socrataQuery);
		SaveData(conf, id, urlStr, socrataQuery);

		if (socrataQuery.length() == 0) {
			return pList;
		}
		LOG.info("SELECT RECORD BY SOCRATA -> ");

		final GetMethod method = new GetMethod(urlStr);
		LOG.info("GET RECORD BY SOCRATA -> " + urlStr);
		LOG.info("<--------------------------->");

		final HttpClient client = new HttpClient();
		method.setRequestHeader("User-Agent", "Hammer Project - Colombo");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Colombo");
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		try {
			int statusCode = client.executeMethod(method);
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			final String temp = GetStringFromInputStream(method.getResponseBodyAsStream());

			// pList = (BasicBSONList) JSON.parse(temp);

			Gson gson = new Gson();
			Type collectionType = new TypeToken<BasicBSONList>() {
			}.getType();
			pList = gson.fromJson(temp, collectionType);

		} catch (Exception e) {
			e.printStackTrace();
			
		} finally {
			method.releaseConnection();
		}

		return pList;
	}
	
	
	/**
	 * Return a dataset from SOCRATA Site
	 * 
	 * @param url
	 * @param datasource
	 * @param id
	 * @param c
	 * @return
	 * @throws Exception
	 */
	public static BasicBSONList GetDataSet(Configuration conf, String id, String url)
			throws Exception {
		BasicBSONList pList = new BasicBSONList();
		LOG.info("SELECT RECORD BY SOCRATA -> ");

		final GetMethod method = new GetMethod(url);
		LOG.info("GET RECORD BY SOCRATA -> " + url);
		LOG.info("<--------------------------->");

		final HttpClient client = new HttpClient();
		method.setRequestHeader("User-Agent", "Hammer Project - Colombo");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - Colombo");
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
		try {
			int statusCode = client.executeMethod(method);
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			final String temp = GetStringFromInputStream(method.getResponseBodyAsStream());

			// pList = (BasicBSONList) JSON.parse(temp);

			Gson gson = new Gson();
			Type collectionType = new TypeToken<BasicBSONList>() {
			}.getType();
			pList = gson.fromJson(temp, collectionType);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			method.releaseConnection();
		}

		return pList;
	}

	/**
	 * Return true if dataset contains column
	 * 
	 * 
	 * @return
	 */
	public static boolean CheckColumn(Configuration conf, String id, String column) {

		MongoClient mongo = null;
		MongoDatabase db = null;
		boolean result = false;
		try {
			MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());
			MongoCollection<Document> dataSet = db.getCollection(inputURI.getCollection());
			BasicDBObject searchQuery = new BasicDBObject().append("_id", id).append("meta", column);
			;

			dataSet.count(searchQuery);
			if (dataSet.count(searchQuery) > 0) {
				result = true;
			}

		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}
		return result;
	}

	public static String CreateWhereCondition(Configuration conf, String id) {

		String query = conf.get("query-string");
		Isabella parser = new Isabella(new StringReader(query));

		String socrataQuery = "";
		try {
			QueryGraph q = parser.queryGraph();
			LOG.debug(query);
			int c = 0;
			int ok = 0;
			while (c < q.getQueryCondition().size()) {
				Edge en = q.getQueryCondition().get(c);
				
				boolean check = CheckColumn(conf, id, en.getName());
				if (check) {
					for (Node ch : en.getChild()) {
						if ((ch instanceof TextValueNode) || (ch instanceof NumberValueNode)) {

							if (ok > 0) {
								socrataQuery += "  " + en.getCondition() + " ";
							}

							if (en.getOperator().equals("eq") && ch instanceof TextValueNode) {
								socrataQuery += en.getName() + " = '" + ch.getName() + "'";
							} else if (en.getOperator().equals("ge") && ch instanceof TextValueNode) {
								socrataQuery += en.getName() + " >= '" + ch.getName() + "'";
							} else if (en.getOperator().equals("eq") && (ch instanceof NumberValueNode)) {
								socrataQuery += en.getName() + " = " + ch.getName();
							} else if (en.getOperator().equals("gt") && (ch instanceof NumberValueNode)) {
								socrataQuery += en.getName() + " > " + ch.getName();
							} else if (en.getOperator().equals("lt") && (ch instanceof NumberValueNode)) {
								socrataQuery += en.getName() + " < " + ch.getName();
							} else if (en.getOperator().equals("ge") && (ch instanceof NumberValueNode)) {
								socrataQuery += en.getName() + " >= " + ch.getName();
							} else if (en.getOperator().equals("le") && (ch instanceof NumberValueNode)) {
								socrataQuery += en.getName() + " <= " + ch.getName();
							} else {
								socrataQuery += en.getName() + " = '" + ch.getName() + "'";
							}
							
							ok++;
							
						}
					}
				}
				
				c++;
			}
		} catch (Exception ex) {
			LOG.error(ex);
		}
		return socrataQuery;
	}

	/**
	 * 
	 * Save the data of the mapper to database
	 * 
	 * @param datasource
	 * @param count
	 */
	private static void SaveData(Configuration conf, String datasource, String link, String searchQuery) {
		MongoClient mongo = null;
		MongoDatabase db = null;

		try {

			MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			if (db.getCollection(conf.get("list-result")) == null) {
				db.createCollection(conf.get("list-result"));
			}

			db.getCollection(conf.get("list-result")).findOneAndUpdate(new Document("_id", datasource),
					new Document("$set", new Document("search_query", searchQuery)));
			db.getCollection(conf.get("list-result")).findOneAndUpdate(new Document("_id", datasource),
					new Document("$set", new Document("res_link", link)));

		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}

	}

	// convert InputStream to String
	private static String GetStringFromInputStream(InputStream is) {

		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();

		String line;
		try {

			br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return sb.toString();

	}
	
	/**
	 * Get Url
	 * @param conf
	 * @param id
	 * @param url
	 * @param offset
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public static String GetUrl(Configuration conf, String id, String url, int offset, int limit, String socrataQuery)
			throws Exception {

		String urlStr = url + "?$offset=" + offset + "&$limit=" + limit + "&$where=" + EncodeURIComponent(socrataQuery);
		return urlStr;
	}

}
