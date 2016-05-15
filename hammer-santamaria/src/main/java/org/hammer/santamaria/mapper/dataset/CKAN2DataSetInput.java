package org.hammer.santamaria.mapper.dataset;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.params.ClientPNames;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.hammer.santamaria.mapper.dataset.utils.DSSUtils;

/**
 * CKAN2 data set reader
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class CKAN2DataSetInput implements DataSetInput {

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

	public static final String PACKAGE_GET = "/package/";

	private static final Log LOG = LogFactory.getLog(CKAN2DataSetInput.class);

	@SuppressWarnings("unchecked")
	public BSONObject getDataSet(String url, String datasource, String id, BSONObject c) {
		BSONObject dataset = new BasicBSONObject();
		dataset.put("datasource", datasource);
		dataset.put("id", id);

		String sId = EncodeURIComponent(id);
		LOG.info("---> id " + id + " - " + sId);

		HttpClient client = new HttpClient();
		client.getHttpConnectionManager().getParams().setParameter(ClientPNames.HANDLE_REDIRECTS, false);

		LOG.info(
				"******************************************************************************************************");
		LOG.info(" ");
		LOG.info(url + PACKAGE_GET + sId);
		LOG.info(" ");
		LOG.info(
				"******************************************************************************************************");

		GetMethod method = new GetMethod(url + PACKAGE_GET + sId);

		method.setRequestHeader("User-Agent", "Hammer Project - SantaMaria crawler");
		method.getParams().setParameter(HttpMethodParams.USER_AGENT, "Hammer Project - SantaMaria crawler");
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

		try {
			int statusCode = client.executeMethod(method);
			if (statusCode != HttpStatus.SC_OK) {
				throw new Exception("Method failed: " + method.getStatusLine());
			}
			byte[] responseBody = method.getResponseBody();
			LOG.debug(new String(responseBody));
			Document result = Document.parse(new String(responseBody));

			if (result != null) {
				dataset.put("title", result.get("title"));
				dataset.put("author", result.get("author"));
				dataset.put("author_email", result.get("author_email"));
				dataset.put("license_id", result.get("license"));
			}

			ArrayList<String> tags = new ArrayList<String>();
			ArrayList<String> meta = new ArrayList<String>();
			ArrayList<String> other_tags = new ArrayList<String>();
			
			if (result.containsKey("author"))
				other_tags.add(result.get("author").toString());
			if (result.containsKey("title"))
				other_tags.addAll(DSSUtils.GetKeyWordsFromText(result.get("title").toString()));
			if (result.containsKey("description"))
				other_tags.addAll(DSSUtils.GetKeyWordsFromText(result.get("description").toString()));

			ArrayList<Document> resources = new ArrayList<Document>();
			if (result != null && result.containsKey("resources")) {
				resources = (ArrayList<Document>) result.get("resources");
				for (Document resource : resources) {
					if (resource.getString("format").toUpperCase().equals("JSON")) {
						dataset.put("dataset-type", "JSON");
						dataset.put("url", resource.get("url"));
						dataset.put("created", resource.get("created"));
						dataset.put("description", resource.get("description"));
						dataset.put("revision_timestamp", resource.get("revision_timestamp"));
						meta = DSSUtils.GetMetaByResource(resource.get("url").toString());

					}
				}
			}

			if (result != null && result.containsKey("tags")) {
				ArrayList<Document> tagsFromCKAN = (ArrayList<Document>) result.get("tags");
				for (Document tag : tagsFromCKAN) {
					if (tag.containsKey("state") && tag.getString("state").toUpperCase().equals("ACTIVE")) {
						tags.add(tag.getString("display_name").trim().toLowerCase());
					} else if (tag.containsKey("display_name")) {
						tags.add(tag.getString("display_name").trim().toLowerCase());
					}
				}

			}

			dataset.put("tags", tags);
			dataset.put("meta", meta);
			dataset.put("resources", resources);
			dataset.put("other_tags", other_tags);

		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e);
		} finally {
			method.releaseConnection();
		}
		return dataset;
	}



}
