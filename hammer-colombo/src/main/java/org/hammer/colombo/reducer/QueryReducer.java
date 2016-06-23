package org.hammer.colombo.reducer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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


	@Override
	protected void setup(Reducer<Text, BSONWritable, Text, BSONWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		LOG.info("SETUP QUERY REDUCE - Hammer Colombo Project");

	}

	@Override
	public void reduce(final Text pKey, final Iterable<BSONWritable> pValues, final Context pContext)
			throws IOException, InterruptedException {

		LOG.debug("START COLOMBO QUERY REDUCER");
		for (BSONWritable b : pValues) {
			pContext.write(new Text(pKey.hashCode() + ""), b);
		}

	

	}
	
}