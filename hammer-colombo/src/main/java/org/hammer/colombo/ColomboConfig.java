package org.hammer.colombo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.hammer.colombo.mapper.ColomboMapper2;
import org.hammer.colombo.reducer.ColomboReducer2;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;

/**
 * Config
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Colombo
 *
 */
public class ColomboConfig extends MongoTool {

	
	public ColomboConfig() {
        this(new Configuration());
    }

    public ColomboConfig(final Configuration conf) {
        setConf(conf);
        
        
        MongoConfigUtil.setInputFormat(conf, ColomboInputFormat.class);
        MongoConfigUtil.setOutputFormat(conf, ColomboOutputFormat.class);
        
        
       
        MongoConfigUtil.setMapper(conf, ColomboMapper2.class);
        MongoConfigUtil.setMapperOutputKey(conf, Text.class);
        MongoConfigUtil.setMapperOutputValue(conf, BSONWritable.class);

        MongoConfigUtil.setReducer(conf, ColomboReducer2.class);
        MongoConfigUtil.setOutputKey(conf, Text.class);
        MongoConfigUtil.setOutputValue(conf, BSONWritable.class);
        
        MongoConfigUtil.setInputURI(conf, "mongodb://192.168.56.90:27017/hammer." + conf.get("dataset-table"));
        
        MongoConfigUtil.setOutputURI(conf, "mongodb://192.168.56.90:27017/hammer." + conf.get("query-table"));

//        MongoConfigUtil.setInputURI(conf, "mongodb://hammerdb-instance-1:27017/hammer.dataset");
//        MongoConfigUtil.setOutputURI(conf, "mongodb://hammerdb-instance-1:27017/hammer." + conf.get("query-table"));

        MongoClient mongo = null;
        MongoDatabase db = null;
        try {
        	MongoClientURI outputURI = MongoConfigUtil.getOutputURI(conf);
			mongo = new MongoClient(outputURI);
			db =  mongo.getDatabase(outputURI.getDatabase());
			System.out.println("COLOMBO Create temp table " + outputURI.getCollection());
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
