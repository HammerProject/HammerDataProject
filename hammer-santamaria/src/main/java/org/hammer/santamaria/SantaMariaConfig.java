package org.hammer.santamaria;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.hammer.santamaria.mapper.SantaMariaMapper;
import org.hammer.santamaria.reducer.SantaMariaReducer;

/**
 * Config
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class SantaMariaConfig extends MongoTool {

	public static final boolean TEST = true;
	
	public SantaMariaConfig() {
        this(new Configuration());
    }

    public SantaMariaConfig(final Configuration conf) {
        setConf(conf);
        MongoConfigUtil.setInputFormat(conf, SantaMariaInputFormat.class);
        MongoConfigUtil.setOutputFormat(conf, SantaMariaOutputFormat.class);
        
        
        MongoConfigUtil.setMapper(conf, SantaMariaMapper.class);
        MongoConfigUtil.setMapperOutputKey(conf, Text.class);
        MongoConfigUtil.setMapperOutputValue(conf, BSONWritable.class);

        MongoConfigUtil.setReducer(conf, SantaMariaReducer.class);
        MongoConfigUtil.setOutputKey(conf, Text.class);
        MongoConfigUtil.setOutputValue(conf, BSONWritable.class);
        
     
        MongoConfigUtil.setInputURI(conf, "mongodb://" + conf.get("mongodb-host") + ":27017/hammer." + conf.get("datasource-table"));
        MongoConfigUtil.setOutputURI(conf, "mongodb://" + conf.get("mongodb-host") + ":27017/hammer.dataset");
     
        
        //MongoConfigUtil.setInputURI(conf, "mongodb://192.168.56.90:27017/hammer." + conf.get("datasource-table"));
        //MongoConfigUtil.setOutputURI(conf, "mongodb://192.168.56.90:27017/hammer.dataset");
        
//        MongoConfigUtil.setInputURI(conf, "mongodb://hammerdb-instance-1:27017/hammer.datasource");
//        MongoConfigUtil.setOutputURI(conf, "mongodb://hammerdb-instance-1:27017/hammer.dataset");
    }
}
