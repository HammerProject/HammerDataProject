package org.hammer.colombo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.hammer.colombo.utils.StatUtils;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.cc.util.IsabellaUtils;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.QueryGraph;

/**
 * Colombo App Start
 * 
 * 
 *
 */
public class App {
	

	/**
	 * Run the Colombo Search Engine
	 * 
	 * @param fileQuery path to file
	 * @param fileSystem hdfs or local
	 * @param searchMode download or search
	 * @param queryMode  labels or keywords
	 * @param thKrm the krm value to cut the resources (during relevant resource search)
	 * @param thRm  the rm value to cut the resources (during sdf phase)
	 * @param thSim the sim value to cut record during apply where condition
	 * 
	 * @throws Exception
	 */
	public static void Run(String fileQuery, String fileSystem, String searchMode, String queryMode, float thKrm,  float thRm, float thSim) throws Exception {
		System.out.println("!!! Hammer Project !!!");
		System.out.println("!!! Colombo Module start.....");
		
		Configuration conf = new Configuration();
		new ColomboConfig(conf);
		conf.set("thesaurus.url", "http://thesaurus.altervista.org/thesaurus/v1");
		conf.set("thesaurus.key", "bVKAPIcUum3hEFGKEBAu"); // x hammerproject
		conf.set("thesaurus.lang", "it_IT");
		// insert a limit to socrata recordset for memory heap problem
		conf.set("socrata.record.limit", "30000");
		conf.set("mongo.splitter.class", "org.hammer.colombo.splitter.DataSetSplitter");


		String query = "";
		conf.set("hdfs-site", "hdfs://192.168.56.90:9000"); //54310???
		conf.set("download", "hdfs://192.168.56.90:9000/hammer/download");
		if (fileSystem.equals("hdfs")) {
			conf.set("query-file", "hdfs://192.168.56.90:9000/hammer/" + fileQuery);
			query = ReadFileFromHdfs(conf);
		} else {
			conf.set("query-file", fileQuery);
			query = IsabellaUtils.readFile(fileQuery);
		}
		
		
		// set the parameter
		conf.set("search-mode", searchMode);
		conf.set("query-mode", queryMode);
		conf.set("query-string", query);
		conf.set("thRm", thRm + "");
		conf.set("thKrm", thKrm + "");
		conf.set("thSim", thSim + "");
		
		// check the query
		Isabella parser = new Isabella(new StringReader(query));
		QueryGraph q;
		try {
			q = parser.queryGraph();
		} catch (ParseException e) {
			throw new IOException(e);
		}
		q.setIndex(StatUtils.GetMyIndex(conf));

		for (IsabellaError err : parser.getErrors().values()) {
			System.out.println(err.toString());
		}
		if (parser.getErrors().size() > 0) {
			throw new IOException("Query syntax not correct.");
		}
		
		// the the paramter from the qeury
		conf.set("query-table", "query" + (q.hashCode() + "").replaceAll("-", "_"));
		conf.set("query-result", "result" + (q.hashCode() + "").replaceAll("-", "_"));
		conf.set("list-result", "list" + (q.hashCode() + "").replaceAll("-", "_"));
		conf.set("stat-result", "stat" + (q.hashCode() + "").replaceAll("-", "_"));
		conf.set("joinCondition", q.getJoinCondition());
		
		System.out.println("******************************************************************");
		System.out.println("******************************************************************");
		System.out.println("******************************************************************");
		System.out.println("COLOMBO Create temp table " + conf.get("query-table"));
		System.out.println("COLOMBO Create result table " + conf.get("query-result"));
		System.out.println("COLOMBO Create list resources " + conf.get("list-result"));
		System.out.println("COLOMBO Create stat output " + conf.get("stat-result"));
		System.out.println("******************************************************************");
		System.out.println("******************************************************************");
		System.out.println("******************************************************************");
		
		
		ToolRunner.run(conf, new ColomboConfig(conf), new String[0]);

	}

	public static void main(String[] pArgs) throws Exception {

		if (pArgs == null || pArgs.length < 7) {
			throw new Exception("Parameter: <path_to_query> <file-system: local|hdfs> <search mode: search|download> <query mode: keywords|labels> <thKrm: 0.5|0.01..>  <thRm: 0.5|0.01..> <thSim: 0.5|0.01..>");
		}
		Run(pArgs[0], pArgs[1], pArgs[2], pArgs[3], Float.parseFloat(pArgs[4]), Float.parseFloat(pArgs[5]), Float.parseFloat(pArgs[6]));
	}

	public static String ReadFileFromHdfs(Configuration conf) {
		FileSystem fs = null;
		BufferedReader br = null;
		StringBuilder sb = null;
		try {
			Path pt = new Path(conf.get("query-file"));
			fs = FileSystem.get(new Configuration());
			br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			sb = new StringBuilder();
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (Exception e) {
				}
			}
			if (br != null) {
				try {
					br.close();
				} catch (Exception e) {
				}
			}

		}

		return sb.toString();
	}


}
