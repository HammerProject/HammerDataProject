package org.hammer.pinta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.hammer.pinta.output.PintaOutputCommiter;


/**
 * App
 *
 */
public class App {

	/**
	 * 
	 * @param thSim --> limit for similarity function
	 * @param recalcIndex --> recalc the index if true, if false only calc similarity
	 * @throws Exception
	 */
	public static void Run(float thSim, boolean recalcIndex) throws Exception {
		System.out.println("!!! Hammer Project !!!");
		System.out.println("!!! Pinta Module start.....");
		Configuration conf = new Configuration();
		conf.set("thesaurus.url", "http://thesaurus.altervista.org/thesaurus/v1");
		conf.set("thesaurus.key", "bVKAPIcUum3hEFGKEBAu"); // x hammerproject
		conf.set("thesaurus.lang", "it_IT");
		conf.set("thSim", thSim + "");
		conf.set("mongo.splitter.class", "org.hammer.pinta.splitter.PintaSplitter");
		new PintaConfig(conf);
		if(recalcIndex) {
			ToolRunner.run(conf, new PintaConfig(conf), new String[0]);
		}
		PintaOutputCommiter.CalcSimTerms(conf);
	}
	
	public static void main(String[] pArgs) throws Exception {

		if (pArgs == null || pArgs.length < 2) {
			throw new Exception("Parameter: <thSim: 0.5|0.01..> <recalcindex: true|false");
		}
		Run(Float.parseFloat(pArgs[0]), Boolean.parseBoolean(pArgs[1]));
	}


}
