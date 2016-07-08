package org.hammer.pinta;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.hammer.pinta.mapper.PintaMapper;
import org.hammer.pinta.reducer.PintaReducer;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;

/**
 * 
 * Config
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Pinta
 *
 */
public class PintaConfig extends MongoTool {

	
	public PintaConfig() {
        this(new Configuration());
    }

    public PintaConfig(final Configuration conf) {
        setConf(conf);
        
        
        MongoConfigUtil.setInputFormat(conf, PintaInputFormat.class);
        MongoConfigUtil.setOutputFormat(conf, PintaOutputFormat.class);
        
        
        MongoConfigUtil.setMapper(conf, PintaMapper.class);
        MongoConfigUtil.setMapperOutputKey(conf, Text.class);
        MongoConfigUtil.setMapperOutputValue(conf, BSONWritable.class);

        MongoConfigUtil.setReducer(conf, PintaReducer.class);
        MongoConfigUtil.setOutputKey(conf, Text.class);
        MongoConfigUtil.setOutputValue(conf, BSONWritable.class);
        MongoConfigUtil.setInputURI(conf, "mongodb://192.168.56.90:27017/hammer.dataset");
        MongoConfigUtil.setOutputURI(conf, "mongodb://192.168.56.90:27017/hammer.index");
        //MongoConfigUtil.setInputURI(conf, "mongodb://hammerdb-instance-1:27017/hammer.dataset");
        //MongoConfigUtil.setOutputURI(conf, "mongodb://hammerdb-instance-1:27017/hammer.index");

    }
    
    
}
