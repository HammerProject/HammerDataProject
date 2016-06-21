package org.hammer.colombo.mapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.Document;
import org.hammer.colombo.utils.SocrataUtils;
import org.hammer.colombo.utils.StatUtils;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Keyword;
import org.hammer.isabella.query.QueryGraph;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * Query Mapper
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Colombo
 *
 */
public class QueryMapper extends Mapper<Object, BSONObject, Text, BSONWritable> {

	public static final Log LOG = LogFactory.getLog(QueryMapper.class);

	private HashMap<String, Keyword> kwIndex = null;
	private Configuration conf = null;

	@Override
	protected void setup(Mapper<Object, BSONObject, Text, BSONWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		LOG.info("SETUP QUERY MAPPER - Hammer Colombo Project");
		this.conf = context.getConfiguration();
		this.kwIndex = StatUtils.GetMyIndex(conf);
	}

	@Override
	public void map(final Object pKey, final BSONObject pValue, final Context pContext)
			throws IOException, InterruptedException {
		LOG.debug("START COLOMBO QUERY MAPPER " + pKey + " --- " + pValue);

		if (pValue != null) {
			LOG.debug("START COLOMBO MAPPER - Dataset " + pKey + " --- " + pValue.hashCode());
			// get the query string from pValue
			String queryString = (String) pValue.get("queryString");
			Isabella parser = new Isabella(new StringReader(queryString));
			QueryGraph query;

			try {
				query = parser.queryGraph();
			} catch (ParseException e) {
				throw new IOException(e);
			}
			query.setIndex(kwIndex);

			for (IsabellaError err : parser.getErrors().values()) {
				LOG.error(err.toString());
			}

			if (parser.getErrors().size() == 0) {
				// search document for this query
				// esecute the getSetList function for every fuzzy query
				// the function return the list of the resources that match with
				// the
				// query
				// we store the data into a dataset map
				// for eliminate the duplicate resource
				MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
				Map<String, Document> dataSet = new HashMap<String, Document>();
				MongoClient mongo = null;
				MongoDatabase db = null;
				try {
					mongo = new MongoClient(inputURI);
					db = mongo.getDatabase(inputURI.getDatabase());
					// connection with dataset and index collection of mongodb
					MongoCollection<Document> dataset = db.getCollection(inputURI.getCollection());
					MongoCollection<Document> index = db.getCollection("index");

					List<Document> temp = getSetList(query, pKey.toString(), dataset, index);

					for (Document t : temp) {
						String documentKey = t.getString("_id");
						dataSet.put(documentKey, t);

						LOG.debug("---> found " + documentKey + " - " + t.getString("title"));
						BasicDBObject resource = new BasicDBObject();
						if (conf.get("search-mode").equals("download")) {
							resource.put("_id", documentKey);
							if (t.containsKey("url") && !t.containsKey("remove")) {
								resource.put("_id", documentKey);
								resource.put("url", t.getString("url"));
								resource.put("dataset-type", t.getString("dataset-type"));
								resource.put("datainput_type", t.getString("datainput_type"));
								resource.put("id", t.getString("id"));

								Text key = new Text(documentKey);
								pContext.write(key, new BSONWritable(resource));

							}

						} else {
							resource.put("_id", documentKey);
							if (t.containsKey("url") && !t.containsKey("remove")) {
								resource.put("_id", documentKey);
								resource.put("url", t.getString("url"));
								resource.put("dataset-type", t.getString("dataset-type"));
								resource.put("datainput_type", t.getString("datainput_type"));
								resource.put("id", t.getString("id"));

								Text key = new Text(documentKey);
								pContext.write(key, new BSONWritable(resource));

							}
						}
					}

				} catch (Exception ex) {
					LOG.error(ex.getMessage());
					LOG.debug(ex);
				} finally {
					if (mongo != null) {
						mongo.close();
					}
				}


			}

		}
	}

	/**
	 * Contacts the db and builds a map of each set by keyword
	 * 
	 * 
	 * @return
	 */
	protected ArrayList<Document> getSetList(QueryGraph q, String keywords, MongoCollection<Document> dataset,
			MongoCollection<Document> index) {

		final ArrayList<Document> returnList = new ArrayList<Document>();
		float thKrm = Float.parseFloat(conf.get("thKrm"));
		float thRm = Float.parseFloat(conf.get("thRm"));

		try {

			// now search the keywords or the labels on the index (with or)
			StringTokenizer st = new StringTokenizer(keywords, ";");

			BasicDBList or = new BasicDBList();
			while (st.hasMoreElements()) {
				String word = st.nextToken().trim().toLowerCase();
				if (word.trim().length() > 2) {
					/*
					 * ArrayList<String> synonyms =
					 * ThesaurusUtils.Get(conf.get("thesaurus.url" ), word,
					 * conf.get("thesaurus.lang"), conf.get("thesaurus.key"),
					 * "json"); synonyms.add(word); for (String synonym :
					 * synonyms) { BasicDBObject temp = new
					 * BasicDBObject("keyword", new BasicDBObject("$regex",
					 * synonym)); or.add(temp); }
					 */

					// BasicDBObject temp = new BasicDBObject("keyword", new
					// BasicDBObject("$regex", word));
					BasicDBObject temp = new BasicDBObject("keyword", word);
					or.add(temp);
				}

			}

			// search the keywords on my index
			BasicDBObject searchQuery = new BasicDBObject("$or", or);
			LOG.debug("Colombo gets data set from database..." + searchQuery.toString());

			FindIterable<Document> indexS = index.find(searchQuery);

			//
			// keyword find on my index
			// the structure of kwFinded
			// key = the keyword
			// value = the list of the documents associate with key
			//
			final HashMap<String, ArrayList<String>> kwFinded = new HashMap<String, ArrayList<String>>();
			indexS.forEach(new Block<Document>() {

				public void apply(final Document document) {
					@SuppressWarnings("unchecked")
					ArrayList<Document> docList = (ArrayList<Document>) document.get("documents");
					ArrayList<String> idList = new ArrayList<String>();
					for (Document doc : docList) {
						idList.add(doc.getString("document"));
					}
					if (docList != null) {
						kwFinded.put(document.getString("keyword"), idList);
					}
				}
			});
			if (kwFinded.size() == 0) {
				throw new Exception("!!!!! ERROR NOTHING FOUND !!!!");
			} else {
				LOG.info(" Found keyword with resources --> " + kwFinded.size());
			}

			// now we find the relevant resources and calculate krm
			// krm = [0,1]
			// krm = number keyword match / total number of keyword
			//
			// the rSet contains the resources
			// the hast map krmMap contains the value of krm for each resources
			BasicDBList rSet = new BasicDBList();
			HashMap<String, Float> krmMap = new HashMap<String, Float>();

			LOG.debug("Get resources and calc krm....");
			for (ArrayList<String> listId : kwFinded.values()) {
				for (String key : listId) {
					float found = 0;
					for (ArrayList<String> lista : kwFinded.values()) {
						if (lista.contains(key)) {
							found++;
						}
					}
					float krm = ((float) found / (float) kwFinded.size());
					//
					// if krm >= th ok!!!
					//
					if (krm >= thKrm) {
						BasicDBObject temp = new BasicDBObject("_id", key);
						if (!krmMap.containsKey(key)) {
							rSet.add(temp);
							krmMap.put(key, krm);
						}
					}
				}
			}

			LOG.info(" ----------------------------------------------> ");
			if (rSet.size() == 0) {
				throw new Exception("!!!!! ERROR NOTHING RESOURCE FOUND !!!!");
			} else {
				LOG.info(" --> fount relevant resources  " + rSet.size());
			}

			// now we calc sdfMap where the map key is the document key and the
			// value is the valure of sdf
			HashMap<String, Float> sdfMap = new HashMap<String, Float>();
			// calc the total weitgth
			final HashMap<String, Float> wWhere = q.getwWhere();
			final Float w = q.getWeightWhere();

			BasicDBObject searchRR = new BasicDBObject("$or", rSet);
			System.out.println("Colombo gets relevant resources..." + searchRR.toString());
			FindIterable<Document> iterable = dataset.find(searchRR);
			iterable.forEach(new Block<Document>() {

				@SuppressWarnings("unchecked")
				public void apply(final Document document) {
					float sdf = 0.0f;
					ArrayList<String> meta = new ArrayList<String>();
					if (document.keySet().contains("meta")) {
						meta = (ArrayList<String>) document.get("meta");
					}
					for (String k : meta) {
						sdf += (wWhere.containsKey(k)) ? wWhere.get(k) : 0.0f;
					}
					sdf = (float) sdf / (float) w;
					sdfMap.put(document.getString("_id"), sdf);

				}
			});

			// now we have sdf and krm for each documents and can calculate rm
			// alfa = 0.6
			// r.rm= (1-alfa) * r.krm) + (alfa * r.sfd)
			//
			// th in this case is set to thRm

			// the rmMap contain the rm-value for each document_id
			//
			HashMap<String, Float> rmMap = new HashMap<String, Float>();
			//
			// the idSet contains the list of resource that are ok!
			//
			BasicDBList idSet = new BasicDBList();
			//
			for (String documentKey : krmMap.keySet()) {
				float krm = krmMap.get(documentKey);
				float sdf = (sdfMap.containsKey(documentKey)) ? sdfMap.get(documentKey) : 0.0f;
				float rm = ((1.0f - 0.6f) * krm) + (0.6f * sdf);
				rmMap.put(documentKey, rm);
				if (rm >= thRm) {
					BasicDBObject temp = new BasicDBObject("_id", documentKey);
					idSet.add(temp);
				}
			}

			if (idSet.size() == 0) {
				throw new Exception("!!!!! ERROR NOTHING RELEVANT RESOURCES FOUND (with rm >= " + thRm + ") !!!!");
			} else {
				LOG.info("--- > FOUND RELEVANT RESOURCES FOUND (with rm >= " + thRm + ") " + idSet.size());
			}

			// search the resources
			// and if i want to search we must add the JSON constrain, else not
			searchQuery = new BasicDBObject("$or", idSet);
			searchQuery.append("dataset-type", new BasicDBObject("$regex", "JSON"));

			System.out.println("Colombo gets dataset from database..." + searchQuery.toString());
			//
			// rmList is the final List of resources!!!
			final ArrayList<Document> rmList = new ArrayList<Document>();
			iterable = dataset.find(searchQuery);
			iterable.forEach(new Block<Document>() {
				public void apply(final Document document) {
					rmList.add(document);

				}
			});

			// before return split check socrata case
			for (Document doc : rmList) {

				// if socrata split in set by 5000 record
				if (doc.containsKey("datainput_type") && doc.get("datainput_type")
						.equals("org.hammer.santamaria.mapper.dataset.SocrataDataSetInput")) {

					String socrataQuery = SocrataUtils.CreateWhereCondition(this.conf, doc.getString("_id"));

					if (socrataQuery.length() > 0 && doc.getString("dataset-type").equals("JSON")) {
						long count = SocrataUtils.CountPackageList(this.conf, doc.getString("url"),
								doc.getString("_id"));
						int offset = 0;

						if (count > 0) {
							while (offset < count) {
								Document tempDoc = new Document(doc);
								String tempUrl = SocrataUtils.GetUrl(this.conf, doc.getString("_id"),
										doc.getString("url"), offset, 1000, socrataQuery);
								tempDoc.replace("url", tempUrl);
								tempDoc.replace("_id", doc.get("_id") + "_" + offset);
								returnList.add(tempDoc);
								// save stat
								StatUtils.UpdateResultList(conf, tempDoc);
								offset = offset + 1000;
							}
						}
					}

				} else {
					// save stat
					StatUtils.UpdateResultList(conf, doc);
					returnList.add(doc);
				}
			}

		} catch (Exception ex) {
			LOG.error(ex.getMessage());
			LOG.debug(ex);
		}
		LOG.info("Colombo find " + returnList.size());
		return returnList;
	}
}
