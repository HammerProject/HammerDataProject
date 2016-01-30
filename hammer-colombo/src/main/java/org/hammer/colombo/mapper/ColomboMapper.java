package org.hammer.colombo.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.types.BasicBSONList;
import org.hammer.colombo.utils.ThesaurusUtils;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * Mapper
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Colombo
 *
 */
public class ColomboMapper extends Mapper<Object, BSONObject, Text, BSONWritable> {
	
	public static final Log LOG = LogFactory.getLog(ColomboMapper.class);
	
	/**
	 * Field for join
	 */
	private Map<String, String> joinCondition = new HashMap<String, String>();
	
	private Configuration conf = null;
	
	@Override
	protected void setup(Mapper<Object, BSONObject, Text, BSONWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		LOG.info("SETUP MAPPER");
		this.conf = context.getConfiguration();
		
		if(context.getConfiguration().get("joinCondition") != null && context.getConfiguration().get("joinCondition").trim().length() > 0) {
			StringTokenizer st = new StringTokenizer(context.getConfiguration().get("joinCondition"), ";");
		
		while (st.hasMoreElements()) {
			String word = st.nextToken().trim().toLowerCase();
			ArrayList<String> synonyms = ThesaurusUtils.Get(context.getConfiguration().get("thesaurus.url"), word,
					context.getConfiguration().get("thesaurus.lang"), context.getConfiguration().get("thesaurus.key"), "json");
			synonyms.add(word);

			for (String synonym : synonyms) {
				if(!joinCondition.containsKey(synonym.toLowerCase().trim())) {
					joinCondition.put(synonym.toLowerCase().trim(), word.toLowerCase().trim());
				}
			}			
		}
		}
	}


	@Override
    public void map( final Object pKey, final BSONObject pValue, final Context pContext ) throws IOException, InterruptedException{
    	LOG.debug("START COLOMBO MAPPER " + pKey + " --- " + pValue);
    	
    	if(pValue != null) {
        	LOG.debug("START COLOMBO MAPPER - Dataset " + pKey + " --- " + pValue.hashCode()); 
        	
        	if(pValue instanceof BasicBSONList) {
        		saveMap((String) pKey, ((BasicBSONList) pValue).toMap().size() );
				BasicBSONList pList = (BasicBSONList) pValue; 
        		for(Object pObj : pList) {
        			Text key = new Text(pObj.hashCode() + "");
        			if(pObj instanceof BSONObject) {
	        			((BSONObject) pObj).put("datasource_id", (String) pKey);
	        			pContext.write(key, new BSONWritable((BSONObject)pObj) );
        			} else if (pObj instanceof com.google.gson.internal.LinkedTreeMap) {
        				@SuppressWarnings("rawtypes")
						com.google.gson.internal.LinkedTreeMap gObj = (com.google.gson.internal.LinkedTreeMap) pObj;
        				BSONObject bObj = new BasicBSONObject();
        				bObj.put("datasource_id", (String) pKey);
        				for(Object gKey: gObj.keySet()) {
        					bObj.put(gKey.toString(), gObj.get(gKey));
        				}
        				pContext.write(key, new BSONWritable(bObj) );
        			}
        		}
        	} else if(pValue instanceof BSONObject) {
        		saveMap((String) pKey, 1);
        		Text key = new Text(pValue.hashCode() + "");
        		pValue.put("datasource_id", (String) pKey);
        		pContext.write(key, new BSONWritable(pValue) );
        	}
    		
        	
        	/*
        	 join example
        	if(pValue instanceof BasicBSONList) {
				final BasicBSONList pList = (BasicBSONList) pValue; 
        		for(Object pObj : pList) {
        			final BSONObject bObj = (BSONObject)pObj;
        			bObj.put("source_split", pKey);
        			for(String field : bObj.keySet()) {
        				if(joinCondition.containsKey(field.toLowerCase().trim())) {
            				final Text key = new Text( (joinCondition.get(field.toLowerCase().trim()) + "_" + bObj.get(field)).hashCode() + "");
            				pContext.write(key, new BSONWritable(bObj) );        					
        				}
        			}
        		}
        	} else {
    			final BSONObject bObj = (BSONObject) pValue;
    			bObj.put("source_split", pKey);
    			for(String field : bObj.keySet()) {
    				if(joinCondition.containsKey(field.toLowerCase().trim())) {
        				final Text key = new Text( (joinCondition.get(field.toLowerCase().trim()) + "_" + bObj.get(field)).hashCode() + "");
        				pContext.write(key, new BSONWritable(bObj) );        					
    				}
    			}
        	}
            */

    	}   
    }
	
	/**
	 * Return the configuration
	 * 
	 * @return
	 */
	private Configuration getConfiguration() {
		return this.conf;
	}
	
	/**
	 * 
	 * Save the data of the mapper to database
	 * 
	 * @param datasource
	 * @param count
	 */
	private void saveMap(String datasource, int count) {
		MongoClient mongo = null;
		MongoDatabase db = null;

		
		try {

			MongoClientURI inputURI = MongoConfigUtil.getInputURI(getConfiguration());
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			if (db.getCollection(getConfiguration().get("list-result")) == null) {
				db.createCollection(getConfiguration().get("list-result"));
			}
			
			
			db.getCollection(getConfiguration().get("list-result")).findOneAndUpdate(new Document("_id", datasource), new Document("$set", new Document("size", count)));


		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}
		
	}
	
	
}
