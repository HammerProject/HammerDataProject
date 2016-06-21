package org.hammer.colombo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.hammer.colombo.mapper.QueryMapper;
import org.hammer.colombo.reducer.QueryReducer;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;

/**
 * Colombo Query Config
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Colombo
 *
 */
public class ColomboQueryConfig extends MongoTool {

	
	public ColomboQueryConfig() {
        this(new Configuration());
    }

    public ColomboQueryConfig(final Configuration conf) {
        setConf(conf);
        
        
        MongoConfigUtil.setInputFormat(conf, ColomboQueryInputFormat.class);
        MongoConfigUtil.setOutputFormat(conf, ColomboQueryOutputFormat.class);
        
        
       
        MongoConfigUtil.setMapper(conf, QueryMapper.class);
        MongoConfigUtil.setMapperOutputKey(conf, Text.class);
        MongoConfigUtil.setMapperOutputValue(conf, BSONWritable.class);

        MongoConfigUtil.setReducer(conf, QueryReducer.class);
        MongoConfigUtil.setOutputKey(conf, Text.class);
        MongoConfigUtil.setOutputValue(conf, BSONWritable.class);
        
        MongoConfigUtil.setInputURI(conf, "mongodb://192.168.56.90:27017/hammer.dataset");
        MongoConfigUtil.setOutputURI(conf, "mongodb://192.168.56.90:27017/hammer." + conf.get("resource-table"));

        MongoClient mongo = null;
        MongoDatabase db = null;
        try {
        	MongoClientURI outputURI = MongoConfigUtil.getOutputURI(conf);
			mongo = new MongoClient(outputURI);
			db =  mongo.getDatabase(outputURI.getDatabase());
			System.out.println("COLOMBO QUERY Create temp table " + outputURI.getCollection());
			if(db.getCollection(outputURI.getCollection()) == null) {
				db.createCollection(outputURI.getCollection());
			}
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }finally {
			if(mongo!=null) { mongo.close();}
		}


    }
    
    
}
