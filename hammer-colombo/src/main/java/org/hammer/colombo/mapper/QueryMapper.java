package org.hammer.colombo.mapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.hammer.colombo.splitter.ColomboRecordReader;
import org.hammer.colombo.utils.JSON;
import org.hammer.colombo.utils.StatUtils;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Keyword;
import org.hammer.isabella.query.QueryGraph;

import com.mongodb.BasicDBList;
import com.mongodb.hadoop.io.BSONWritable;

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
				
				
				
				Text key = new Text(columnName + "|" + bsonObj.get(columnName));
				pContext.write(key, new BSONWritable(bsonObj));

			}
			
		}
	}


}
