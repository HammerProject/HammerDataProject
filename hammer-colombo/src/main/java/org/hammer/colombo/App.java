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
		
		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();
		new ColomboQueryConfig(conf1);
		new ColomboConfig2(conf2);
		
		conf2.set("thesaurus.url", "http://thesaurus.altervista.org/thesaurus/v1");
		conf2.set("thesaurus.key", "bVKAPIcUum3hEFGKEBAu"); // x hammerproject
		conf2.set("thesaurus.lang", "it_IT");
		// insert a limit to socrata recordset for memory heap problem
		conf1.set("socrata.record.limit", "30000");
		conf2.set("socrata.record.limit", "30000");

		String query = "";
		conf1.set("hdfs-site", "hdfs://192.168.56.90:9000"); //54310???
		conf1.set("download", "hdfs://192.168.56.90:9000/hammer/download");
		conf2.set("hdfs-site", "hdfs://192.168.56.90:9000"); //54310???
		conf2.set("download", "hdfs://192.168.56.90:9000/hammer/download");
		if (fileSystem.equals("hdfs")) {
			conf1.set("query-file", "hdfs://192.168.56.90:9000/hammer/" + fileQuery);
			conf2.set("query-file", "hdfs://192.168.56.90:9000/hammer/" + fileQuery);
			query = ReadFileFromHdfs(conf1);
		} else {
			conf1.set("query-file", fileQuery);
			conf2.set("query-file", fileQuery);
			query = IsabellaUtils.readFile(fileQuery);
		}
		
		
		// set the parameter
		conf1.set("search-mode", searchMode);
		conf1.set("query-mode", queryMode);
		conf1.set("query-string", query);
		conf1.set("thRm", thRm + "");
		conf1.set("thKrm", thKrm + "");
		conf1.set("thSim", thSim + "");
		
		conf2.set("search-mode", searchMode);
		conf2.set("query-mode", queryMode);
		conf2.set("query-string", query);
		conf2.set("thRm", thRm + "");
		conf2.set("thKrm", thKrm + "");
		conf2.set("thSim", thSim + "");
		
		// check the query
		Isabella parser = new Isabella(new StringReader(query));
		QueryGraph q;
		try {
			q = parser.queryGraph();
		} catch (ParseException e) {
			throw new IOException(e);
		}
		q.setIndex(StatUtils.GetMyIndex(conf1));

		for (IsabellaError err : parser.getErrors().values()) {
			System.out.println(err.toString());
		}
		if (parser.getErrors().size() > 0) {
			throw new IOException("Query syntax not correct.");
		}
		
		// the the paramter from the query
		conf1.set("query-table", "query" + (q.hashCode() + "").replaceAll("-", "_"));
		conf1.set("list-result", "list" + (q.hashCode() + "").replaceAll("-", "_"));
		conf1.set("resource-table", "resource" + (q.hashCode() + "").replaceAll("-", "_"));
		conf1.set("stat-result", "stat" + (q.hashCode() + "").replaceAll("-", "_"));
		conf1.set("joinCondition", q.getJoinCondition());
		
		conf2.set("query-table", "query" + (q.hashCode() + "").replaceAll("-", "_"));
		conf2.set("list-result", "list" + (q.hashCode() + "").replaceAll("-", "_"));
		conf2.set("resource-table", "resource" + (q.hashCode() + "").replaceAll("-", "_"));
		conf2.set("stat-result", "stat" + (q.hashCode() + "").replaceAll("-", "_"));
		conf2.set("joinCondition", q.getJoinCondition());

		
		System.out.println("******************************************************************");
		System.out.println("******************************************************************");
		System.out.println("******************************************************************");
		System.out.println("COLOMBO Create temp table " + conf1.get("query-table"));
		System.out.println("COLOMBO Create resources table " + conf1.get("resource-table"));
		System.out.println("COLOMBO Create list resources " + conf1.get("list-result"));
		System.out.println("COLOMBO Create stat output " + conf1.get("stat-result"));
		System.out.println("******************************************************************");
		System.out.println("******************************************************************");
		System.out.println("******************************************************************");
		
		// 1phase MapReduce --> calc the fuzzy-query list and populate the resource-table
		ToolRunner.run(conf1, new ColomboQueryConfig(conf1), new String[0]);
		
		// 2phase MapReduce --> download the resource-table, apply the selection model (with fuzzy logic)
		// and return the record in query-table
		ToolRunner.run(conf2, new ColomboConfig2(conf2), new String[0]);

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
