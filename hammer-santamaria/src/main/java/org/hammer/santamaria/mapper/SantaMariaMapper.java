package org.hammer.santamaria.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.hammer.santamaria.mapper.dataset.DataSetInput;

import com.mongodb.hadoop.io.BSONWritable;

/**
 * Mapper
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class SantaMariaMapper extends Mapper<Object, BSONObject, Text, BSONWritable> {
	
	public static final Log LOG = LogFactory.getLog(SantaMariaMapper.class);
	
	
	@Override
    public void map( final Object pKey, final BSONObject pValue, final Context pContext ) throws IOException, InterruptedException{
    	LOG.debug("START SANTA MARIA MAPPER " + pKey + " --- " + pValue);
    	
    	if(pValue != null && pValue.keySet() != null && pValue.keySet().contains("datasource") && pValue.keySet().contains("datasource")) {
        	LOG.debug("START SANTA MARIA MAPPER - Dataset " + pValue.get("datasource") + " --- " + pValue.get("dataset"));    		
        	Class<?> c;
            DataSetInput t;
            BSONObject dataset = new BasicBSONObject();
    		try {
    			c = Class.forName((String) pValue.get("datainput_type"));
    			c.getDeclaredConstructors();
    			t = (DataSetInput) c.newInstance();
    			dataset = t.getDataSet((String) pValue.get("url"),(String)  pValue.get("datasource"),(String)  pValue.get("dataset"), pValue);
    		} catch (Exception e) {
    			e.printStackTrace();
    			throw new IllegalStateException("Creation of a new DataSetInput error: " + e.toString());
    		}
    		
    		if(dataset != null) {
	    		Text key = new Text(pValue.get("datasource") + "-" + pValue.get("dataset"));
	            pContext.write(key, new BSONWritable(dataset) );
    		}

    	}
        
    }
}
