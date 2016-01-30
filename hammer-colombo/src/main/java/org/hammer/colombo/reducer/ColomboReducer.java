package org.hammer.colombo.reducer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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

		LOG.debug("START COLOMBO REDUCER");
//		List<BSONWritable> myList = IteratorUtils.toList(pValues.iterator());

		/* join example
		try {
			for (final BSONWritable value : pValues) {
				for (final BSONWritable jValue : pValues) {
					if (!value.getDoc().get("source_split").equals(jValue.getDoc().get("source_split"))) {
						value.getDoc().putAll(jValue.getDoc());
					}
				}
				pContext.write(new Text(value.hashCode() + ""), value);
			}
		} catch (Exception ex) {
			LOG.error(ex);
		}
		*/
		for (final BSONWritable value : pValues) {
			
			pContext.write(new Text(value.hashCode() + ""), value);
			
		}
		

		LOG.debug("COLOMBO - FOUND AND WRITE " + pKey + " DATASET ");

	}
}