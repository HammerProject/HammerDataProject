package org.hammer.colombo.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.hammer.colombo.utils.StatUtils;

import com.mongodb.hadoop.io.BSONWritable;

/**
 * New Version of Colombo Mapper
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Colombo
 *
 */
public class ColomboMapper2 extends Mapper<Object, BSONObject, Text, BSONWritable> {

	public static final Log LOG = LogFactory.getLog(ColomboMapper2.class);

	private Configuration conf = null;

	@Override
	protected void setup(Mapper<Object, BSONObject, Text, BSONWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		LOG.info("SETUP MAPPER2 - Hammer Colombo Project");
		this.conf = context.getConfiguration();

	}

	@Override
	public void map(final Object pKey, final BSONObject pValue, final Context pContext)
			throws IOException, InterruptedException {
		LOG.debug("START COLOMBO MAPPER " + pKey + " --- " + pValue);

		if (pValue != null) {
			LOG.debug("START COLOMBO MAPPER - Dataset " + pKey + " --- " + pValue.hashCode());

			if (conf.get("search-mode").equals("download")) {
				pContext.write(new Text((String) "size"), new BSONWritable(pValue));

			} else {
				// first case pValue is a list of record

				if (pValue instanceof BasicBSONList) {
					// save stat
					BSONObject statObj = new BasicBSONObject();
					statObj.put("type", "resource");
					statObj.put("name", (String) pKey);
					statObj.put("count", ((BasicBSONList) pValue).toMap().size());
					StatUtils.SaveStat(this.conf, statObj);

					BasicBSONList pList = (BasicBSONList) pValue;
					// for each record we store a key-value
					// - key = column-value
					// - value = the record
					for (Object pObj : pList) {

						if (pObj instanceof BSONObject) {
							BSONObject bsonObj = (BSONObject) pObj;
							bsonObj.put("datasource_id", (String) pKey);
							for (Object column : bsonObj.keySet()) {
								String columnName = (String) column;
								Text key = new Text(columnName + "|" + bsonObj.get(columnName));
								pContext.write(key, new BSONWritable(bsonObj));
							}
						} else if (pObj instanceof com.google.gson.internal.LinkedTreeMap) {
							@SuppressWarnings("rawtypes")
							com.google.gson.internal.LinkedTreeMap gObj = (com.google.gson.internal.LinkedTreeMap) pObj;

							BSONObject bsonObj = new BasicBSONObject();
							for (Object gKey : gObj.keySet()) {
								bsonObj.put(gKey.toString(), gObj.get(gKey));
							}
							bsonObj.put("datasource_id", (String) pKey);
							for (Object column : bsonObj.keySet()) {
								String columnName = (String) column;
								Text key = new Text(columnName + "|" + bsonObj.get(columnName));
								pContext.write(key, new BSONWritable(bsonObj));
							}
						}
					}
				}
				// second case pValue is a record
				else if (pValue instanceof BSONObject) {
					// save stat
					BSONObject statObj = new BasicBSONObject();
					statObj.put("type", "resource");
					statObj.put("name", (String) pKey);
					statObj.put("count", 1);
					StatUtils.SaveStat(this.conf, statObj);

					BSONObject bsonObj = (BSONObject) pValue;
					bsonObj.put("datasource_id", (String) pKey);
					for (Object column : bsonObj.keySet()) {
						String columnName = (String) column;
						Text key = new Text(columnName + "|" + bsonObj.get(columnName));
						pContext.write(key, new BSONWritable(bsonObj));
					}

				}

			}

		}
	}



}
