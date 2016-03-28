package org.hammer.santamaria;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Start Santa Maria crawler in map-reduce enviroment
 *
 *
 *
 */
public class App {
	public static void main(String[] pArgs) throws Exception {
		System.out.println("!!! Hammer Project !!!");
		System.out.println("!!! Santa Maria Module start.....");

		Configuration conf = new Configuration();
		conf.set("mongo.splitter.class","org.hammer.santamaria.splitter.DataSourceSplitter");

		System.exit(ToolRunner.run(conf, new SantaMariaConfig(conf), pArgs));
	}
	
	
}
