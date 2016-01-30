package org.hammer.pinta.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;

/**
 * Mapper
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Pinta
 *
 */
public class PintaMapper extends Mapper<Object, BSONObject, Text, BSONWritable> {

	public static final Log LOG = LogFactory.getLog(PintaMapper.class);

	@SuppressWarnings("unchecked")
	@Override
	public void map(final Object pKey, final BSONObject pValue, final Context pContext)
			throws IOException, InterruptedException {
		LOG.debug("START PINTA MAPPER " + pKey + " --- " + pValue.hashCode());
		if (pValue != null) {
			//map all meta
			ArrayList<String> meta = new ArrayList<String>();
			if (pValue.keySet().contains("meta")) {
				meta = (ArrayList<String>) pValue.get("meta");
			}
			ArrayList<String> tags = new ArrayList<String>();
			if (pValue.keySet().contains("tags")) {
				tags = (ArrayList<String>) pValue.get("tags");
			}

			int metaCount = ((meta != null) ? meta.size()  : 0 )+ ((tags != null) ? tags.size()  : 0 );
			if (meta != null) {
				for (String keyword : meta) {
					StringTokenizer st = new StringTokenizer(keyword, " ");
					while (st.hasMoreElements()) {
						String tW = st.nextToken();
						StringTokenizer st1 = new StringTokenizer(tW, "-");
						while (st1.hasMoreElements()) {
							String word = st1.nextToken();
							BasicDBObject temp = new BasicDBObject("keyword", word).append("document",
									pValue.get("document")).append("score", 1 / metaCount);
							pContext.write(new Text(word + ""), new BSONWritable(temp));
						}
					}
				}
			}
			
			if (tags != null) {
				for (String keyword : tags) {
					StringTokenizer st = new StringTokenizer(keyword, " ");
					while (st.hasMoreElements()) {
						String tW = st.nextToken();
						StringTokenizer st1 = new StringTokenizer(tW, "-");
						while (st1.hasMoreElements()) {
							String word = st1.nextToken();
							BasicDBObject temp = new BasicDBObject("keyword", word).append("document",
									pValue.get("document")).append("score", 1 / metaCount);
							pContext.write(new Text(word + ""), new BSONWritable(temp));
						}
					}
				}
			}
		}
	}

}
