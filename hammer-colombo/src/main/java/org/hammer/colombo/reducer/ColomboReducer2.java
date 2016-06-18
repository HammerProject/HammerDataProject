package org.hammer.colombo.reducer;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
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

import com.mongodb.hadoop.io.BSONWritable;

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
			StringTokenizer st = new StringTokenizer(pKey.toString(), "|");
			String column = st.nextToken().toLowerCase();
			String value = st.nextToken().toLowerCase();

			boolean c = false;
			for (Edge en : q.getQueryCondition()) {
				for (Node ch : en.getChild()) {

					if ((ch instanceof ValueNode) && en.getCondition().equals("OR")) {

						double sim = JaroWinkler.Apply(en.getName().toLowerCase(), column.toLowerCase());

						if (sim > thSim) {
							if (en.getOperator().equals("=") && ch.getName().toLowerCase().equals(value)) {
								c = true;
							} else if (en.getOperator().equals(">")) {
								if (ch.getName().toLowerCase().compareTo(value) > 0) {
									c = true;
								}
							} else if (en.getOperator().equals("<")) {
								if (ch.getName().toLowerCase().compareTo(value) < 0) {
									c = true;
								}
							} else if (en.getOperator().equals(">=")) {
								if (ch.getName().toLowerCase().compareTo(value) >= 0) {
									c = true;
								}
							} else if (en.getOperator().equals("<=")) {
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

			long size = 0;
			long totalRecord = 0;
			long selectedRecord = 0;
			long count = 1;

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
			statObj.put("resource-count", count);
			statObj.put("size", size);
			statObj.put("fuzzy-query", 0);
			StatUtils.SaveStat(this.conf, statObj);

			LOG.debug("COLOMBO REDUCE2 - FOUND AND WRITE " + pKey + " DATASET ");
		}

	}
}