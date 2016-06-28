package org.hammer.pinta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;


/**
 * App
 *
 */
public class App {

	public static void Run(float thSim) throws Exception {
		System.out.println("!!! Hammer Project !!!");
		System.out.println("!!! Pinta Module start.....");
		Configuration conf = new Configuration();
		conf.set("thesaurus.url", "http://thesaurus.altervista.org/thesaurus/v1");
		conf.set("thesaurus.key", "bVKAPIcUum3hEFGKEBAu"); // x hammerproject
		conf.set("thesaurus.lang", "it_IT");
		conf.set("thSim", thSim + "");
		
		conf.set("mongo.splitter.class", "org.hammer.pinta.splitter.PintaSplitter");
		ToolRunner.run(conf, new PintaConfig(conf), new String[0]);
		
	}
	
	public static void main(String[] pArgs) throws Exception {

		if (pArgs == null || pArgs.length < 1) {
			throw new Exception("Parameter: <thSim: 0.5|0.01..>");
		}
		Run(Float.parseFloat(pArgs[0]));
	}



}
