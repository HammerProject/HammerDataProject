package org.hammer.santamaria;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.hammer.santamaria.twitter.TwitterThread;


/**
 * Start Santa Maria crawler in map-reduce enviroment
 *
 *
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Santa Maria
 *
 */
public class App {
	public static void main(String[] pArgs) throws Exception {
		System.out.println("!!! Hammer Project !!!");
		System.out.println("!!! Santa Maria Module start.....");

		if(pArgs == null || pArgs.length < 1) {
			throw new Exception("Parameter: <datasource collection|mongodbhost> | tweets <box>");
		}
		if(pArgs[0].trim().equals("tweets")) {
			if(pArgs.length < 2) {
				throw new Exception("Parameter: tweets <box>");
			}
			TwitterThread myTh = new TwitterThread(pArgs[1].trim().toLowerCase());
	        myTh.run();
		} else {
			Run(pArgs[0],pArgs[1]);
		}
		
	}
	
	/**
	 * Run 
	 * @param datasource 
	 * @throws Exception
	 */
	public static void Run(String datasource, String mongodb) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mongo.splitter.class","org.hammer.santamaria.splitter.DataSourceSplitter");
		conf.set("datasource-table", datasource);
		conf.set("mongodb-host", mongodb);
		System.exit(ToolRunner.run(conf, new SantaMariaConfig(conf), new String[0]));
	}
}
