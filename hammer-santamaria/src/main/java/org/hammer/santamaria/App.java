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

		if(pArgs == null || pArgs.length < 1) {
			throw new Exception("Parameter: <datasource collection>");
		}
		Run(pArgs[0]);
		
		
	}
	
	/**
	 * Run 
	 * @param datasource 
	 * @throws Exception
	 */
	public static void Run(String datasource) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mongo.splitter.class","org.hammer.santamaria.splitter.DataSourceSplitter");
		conf.set("datasource-table", datasource);
		
		System.exit(ToolRunner.run(conf, new SantaMariaConfig(conf), new String[0]));
	}
}
