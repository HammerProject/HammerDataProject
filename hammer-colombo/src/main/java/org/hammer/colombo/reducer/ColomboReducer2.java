package org.hammer.colombo.reducer;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.hammer.colombo.utils.StatUtils;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.fuzzy.JaroWinkler;
import org.hammer.isabella.query.Edge;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Keyword;
import org.hammer.isabella.query.Node;
import org.hammer.isabella.query.QueryGraph;
import org.hammer.isabella.query.ValueNode;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * Reducer The second version (in combine with ColomboMapper2)
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Colombo
 *
 */
public class ColomboReducer2 extends Reducer<Text, BSONWritable, Text, BSONWritable> {

	public static final Log LOG = LogFactory.getLog(ColomboReducer2.class);

	private Configuration conf = null;
	private QueryGraph q = null;
	private double thSim = 0.0d;

	@Override
	protected void setup(Reducer<Text, BSONWritable, Text, BSONWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		LOG.info("SETUP REDUCE2 - Hammer Colombo Project");
		this.conf = context.getConfiguration();
		final HashMap<String, Keyword> kwIndex = StatUtils.GetMyIndex(conf);
		thSim = Precision.round(Double.parseDouble(conf.get("thSim")), 2);
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
			for (final BSONWritable value : pValues) {
				size += (value.getDoc().containsField("size")) ? (Long) value.getDoc().get("size") : 0;
				record += (value.getDoc().containsField("record-total")) ? (Long) value.getDoc().get("record-total")
						: 0;
				selectedRecord += (value.getDoc().containsField("record-selected"))
						? (Long) value.getDoc().get("record-selected") : 0;
			}

			// save the stat
			BSONObject statObj = new BasicBSONObject();
			statObj.put("type", "stat");
			statObj.put("record-total", record);
			statObj.put("record-selected", selectedRecord);
			statObj.put("resource-count", 0);
			statObj.put("size", size);
			statObj.put("fuzzy-query", 0);
			statObj.put("total-query", 0);
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
			
			String value = "";
			if(st.hasMoreTokens()) {
				value = st.nextToken().toLowerCase();
			}
			long size = 0;
			long totalRecord = 0;
			long selectedRecord = 0;

			boolean orCheck = false;
			int orCount = 0;
			for (Edge en : q.getQueryCondition()) {
				LOG.info(en.getCondition());
				LOG.info(en.getOperator());
				LOG.info("------------------------------------");
				for (Node ch : en.getChild()) {

					if ((ch instanceof ValueNode) && en.getCondition().equals("or")) {
						orCount++;

						LOG.info(en.getName().toLowerCase() + " -- " + column.toLowerCase());
						LOG.info(ch.getName().toLowerCase() + " -- " + value);

						boolean syn = checkSynset(en.getName().toLowerCase(), column.toLowerCase(), conf);
						double sim = JaroWinkler.Apply(en.getName().toLowerCase(), column.toLowerCase());

						LOG.info("test " + sim + ">=" + thSim + " -- syn: " + syn);
						if ((sim >= thSim) || (syn)) {
							LOG.info("ok sim --> " + sim);
							LOG.info("ok syn --> " + syn);

							LOG.info("check  --> " + ch.getName().toLowerCase().compareTo(value));
							double simV = JaroWinkler.Apply(ch.getName().toLowerCase(), value.toLowerCase());
							LOG.info("check  --> " + simV);
							if (column.toLowerCase().contains("date")) {
								// it's a date
								try {
									SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
									Date dx = formatDate.parse(value.toLowerCase());
									Date sx = formatDate.parse(ch.getName().toLowerCase());

									if (en.getOperator().equals("eq") && sx.equals(dx)) {
										orCheck = true;
									} else if (en.getOperator().equals("gt")) {
										if (sx.compareTo(dx) > 0) {
											orCheck = true;
										}
									} else if (en.getOperator().equals("lt")) {
										if (sx.compareTo(dx) < 0) {
											orCheck = true;
										}
									} else if (en.getOperator().equals("ge")) {
										if (sx.compareTo(dx) >= 0) {
											orCheck = true;
										}
									} else if (en.getOperator().equals("le")) {
										if (sx.compareTo(dx) <= 0) {
											orCheck = true;
										}
									}
								} catch (Exception ex) {
									LOG.error(ex);
								}

							} else if (en.getOperator().equals("eq")
									&& (ch.getName().toLowerCase().equals(value) || (simV >= thSim))) {
								orCheck = true;
							} else if (en.getOperator().equals("gt")) {
								if (ch.getName().toLowerCase().compareTo(value) > 0) {
									orCheck = true;
								}
							} else if (en.getOperator().equals("lt")) {
								if (ch.getName().toLowerCase().compareTo(value) < 0) {
									orCheck = true;
								}
							} else if (en.getOperator().equals("ge")) {
								if (ch.getName().toLowerCase().compareTo(value) >= 0) {
									orCheck = true;
								}
							} else if (en.getOperator().equals("le")) {
								if (ch.getName().toLowerCase().compareTo(value) <= 0) {
									orCheck = true;
								}
							} else if (en.getName().equals(column)) {
								if (ch.getName().toLowerCase().compareTo(value) == 0) {
									orCheck = true;
								}
							}
						}

					}

				}
			}

			for (final BSONWritable record : pValues) {
				if (orCheck || (orCount == 0)) {
					pContext.write(new Text(record.hashCode() + ""), record);
					selectedRecord++;
					size += record.getDoc().toString().length();
					totalRecord++;
				}
			}

			// save the stat
			BSONObject statObj = new BasicBSONObject();
			statObj.put("type", "stat");
			statObj.put("record-total", totalRecord);
			statObj.put("record-selected", selectedRecord);
			statObj.put("resource-count", 0);
			statObj.put("size", size);
			statObj.put("fuzzy-query", 0);
			statObj.put("total-query", 0);
			StatUtils.SaveStat(this.conf, statObj);

			LOG.debug("COLOMBO REDUCE2 - FOUND AND WRITE " + pKey + " DATASET ");
		}

	}

	private Map<String, List<String>> synset = new HashMap<String, List<String>>();

	/**
	 * Verify if field is in synset of column
	 * 
	 * @param column
	 *            the column
	 * @param field
	 *            the field
	 * @param conf
	 *            the configuration for access hadoop
	 * @return true or false
	 */
	private boolean checkSynset(String column, String field, Configuration conf) {

		if (synset.containsKey(column)) {
			List<String> mySynSet = synset.get(column.toLowerCase());
			return mySynSet.contains(field.toLowerCase());
		}

		boolean check = false;
		MongoClient mongo = null;
		MongoDatabase db = null;
		try {
			MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());
			MongoCollection<Document> myIdx = db.getCollection(conf.get("index-table") + "");
			BasicDBObject searchQuery = new BasicDBObject().append("keyword", column.toLowerCase());
			FindIterable<Document> myTerm = myIdx.find(searchQuery);
			if (myTerm.iterator().hasNext()) {
				Document obj = myTerm.iterator().next();
				@SuppressWarnings("unchecked")
				ArrayList<Document> dbSynSet = (ArrayList<Document>) obj.get("syn-set");
				ArrayList<String> mySynSet = new ArrayList<String>();
				if (mySynSet != null) {
					for (Document o : dbSynSet) {
						mySynSet.add((o.get("term") + "").toLowerCase());
					}
				}
				synset.put(column.toLowerCase(), mySynSet);

			}

			if (synset.containsKey(column)) {
				List<String> mySynSet = synset.get(column.toLowerCase());
				check = mySynSet.contains(field.toLowerCase());
			}

		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
			LOG.error(ex.getMessage());
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}

		return check;
	}

}