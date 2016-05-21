package org.hammer.colombo.reducer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;

/**
 * Reducer
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Colombo
 *
 */
public class ColomboReducer extends Reducer<Text, BSONWritable, Text, BSONWritable> {

	public static final Log LOG = LogFactory.getLog(ColomboReducer.class);

	@Override
	public void reduce(final Text pKey, final Iterable<BSONWritable> pValues, final Context pContext)
			throws IOException, InterruptedException {
		Configuration conf = pContext.getConfiguration();
		
		LOG.debug("START COLOMBO REDUCER");
		// List<BSONWritable> myList = IteratorUtils.toList(pValues.iterator());

		/*
		 * join example try { for (final BSONWritable value : pValues) { for
		 * (final BSONWritable jValue : pValues) { if
		 * (!value.getDoc().get("source_split").equals(jValue.getDoc().get(
		 * "source_split"))) { value.getDoc().putAll(jValue.getDoc()); } }
		 * pContext.write(new Text(value.hashCode() + ""), value); } } catch
		 * (Exception ex) { LOG.error(ex); }
		 */

		if (conf.get("search-mode").equals("download")) {
			long size = 0;
			long record = 0;
			long selectedRecord = 0;
			int count = 0;
			for (final BSONWritable value : pValues) {
				size += (value.getDoc().containsField("size")) ? (Long)value.getDoc().get("size") : 0;
				record += (value.getDoc().containsField("record")) ? (Long)value.getDoc().get("record") : 0;
				selectedRecord += (value.getDoc().containsField("selectedRecord")) ? (Long)value.getDoc().get("selectedRecord") : 0;

				count ++;
			}
			BasicDBObject temp = new BasicDBObject();
			temp.put("size", size);
			temp.put("record", record);
			temp.put("selectedRecord", selectedRecord);
			temp.put("count", count);
			
			pContext.write(new Text("size"), new BSONWritable(temp));
		} else {
			for (final BSONWritable value : pValues) {

				pContext.write(new Text(value.hashCode() + ""), value);

			}

			LOG.debug("COLOMBO - FOUND AND WRITE " + pKey + " DATASET ");
		}

	}
}