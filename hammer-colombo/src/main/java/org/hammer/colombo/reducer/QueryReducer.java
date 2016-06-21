package org.hammer.colombo.reducer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.hammer.colombo.utils.StatUtils;

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

	@Override
	protected void setup(Reducer<Text, BSONWritable, Text, BSONWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		LOG.info("SETUP QUERY REDUCE - Hammer Colombo Project");
		this.conf = context.getConfiguration();

	}

	@Override
	public void reduce(final Text pKey, final Iterable<BSONWritable> pValues, final Context pContext)
			throws IOException, InterruptedException {

		LOG.debug("START COLOMBO QUERY REDUCER");

		int c = 0;
		for (BSONWritable b : pValues) {
			pContext.write(new Text(pKey.hashCode() + ""), b);
			c++;
		}
		// save the stat
		BSONObject statObj = new BasicBSONObject();
		statObj.put("type", "stat");
		statObj.put("record-total", 0);
		statObj.put("record-selected", 0);
		statObj.put("resource-count", c);
		statObj.put("size", 0);
		statObj.put("fuzzy-query", 0);
		StatUtils.SaveStat(this.conf, statObj);

	

	}
	
}