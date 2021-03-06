package org.hammer.colombo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Date;

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
 * New
 * 
 *
 */
public class App {
	

	/**
	 * 
	 * Run the Colombo Search Engine
	 * 
	 * @param fileQuery path to file
	 * @param fileSystem hdfs or local
	 * @param searchMode download or search
	 * @param queryMode  labels or keywords
	 * @param thKrm the krm value to cut the resources (during relevant resource search)
	 * @param thRm  the rm value to cut the resources (during sdf phase)
	 * @param thSim the sim value to cut record during apply where condition
	 * @param maxSim max number of sim
	 * @param datasetTable the dataset table
	 * @param indexTable the index table
	 * @param thQuery the limit used to prunning the set of query (using cos(theta) function)
	 * @throws Exception
	 */
	public static void Run(String fileQuery, String fileSystem, String searchMode, String queryMode, float thKrm,  float thRm, float thSim, int maxSim, String datasetTable, String indexTable, int thQuery) throws Exception {
		System.out.println("!!! Hammer Project !!!");
		System.out.println("!!! Colombo Module start.....");
		
		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();
		new ColomboQueryConfig(conf1);
		new ColomboConfig2(conf2);
		
		// set word net home
		String wnHome = "/home/hadoop/software/WordNet-3.0";
		String word2vecmodel = "/hammer/word2vec_hammer_ny/word2vecModel_hammer_ny";
		conf1.set("wn-home", wnHome);
		conf2.set("wn-home", wnHome);
		conf1.set("word2vecmodel", word2vecmodel);
		conf2.set("word2vecmodel", word2vecmodel);
		
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
		conf1.set("maxSim", maxSim + "");
		conf1.set("dataset-table", datasetTable + "");
		conf1.set("index-table", indexTable + "");
		conf1.set("thQuery", thQuery + "");
		
		conf2.set("search-mode", searchMode);
		conf2.set("query-mode", queryMode);
		conf2.set("query-string", query);
		conf2.set("thRm", thRm + "");
		conf2.set("thKrm", thKrm + "");
		conf2.set("thSim", thSim + "");
		conf2.set("maxSim", maxSim + "");
		conf2.set("dataset-table", datasetTable + "");
		conf2.set("index-table", indexTable + "");
		conf2.set("thQuery", thQuery + "");



		// check the query
		Isabella parser = new Isabella(new StringReader(query));
		QueryGraph q;
		try {
			q = parser.queryGraph();
		} catch (ParseException e) {
			throw new IOException(e);
		}
		q.setIndex(StatUtils.GetMyIndex(conf1));
		q.setWnHome(wnHome);

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
		ColomboQueryConfig cq = new ColomboQueryConfig(conf1);
		ToolRunner.run(conf1, cq, new String[0]);
		System.out.println("START-STOP --> STOP VSM Data Set Retrieval " + (new Date()));
		long start = cq.getConf().getLong("start_time", 0);
		long seconds = ((new Date()).getTime() - start)/1000;
		System.out.println("START-STOP --> TIME VSM Data Set Retrieval " + seconds);
		
		// 2phase MapReduce --> download the resource-table, apply the selection model (with fuzzy logic)
		// and return the record in query-table
		ToolRunner.run(conf2, new ColomboConfig2(conf2), new String[0]);

		start = conf2.getLong("start_time", 0);
		seconds = ((new Date()).getTime() - start)/1000;

		System.out.println("START-STOP --> STOP Instance Filtering " + (new Date()));
		System.out.println("START-STOP --> TIME Instance Filtering " + seconds);
	}

	public static void main(String[] pArgs) throws Exception {

		if (pArgs == null || pArgs.length < 11) {
			throw new Exception("Parameter: <path_to_query> <file-system: local|hdfs> <search mode: search|download> <query mode: keywords|labels> <thKrm: 0.5|0.01..>  <thRm: 0.5|0.01..> <thSim: 0.5|0.01..> <maxSim: 1|2|3...> <dataset-table> <index-table> <cosTh:0.9991|0.9992>");
		}
		Run(pArgs[0], pArgs[1], pArgs[2], pArgs[3], Float.parseFloat(pArgs[4]), Float.parseFloat(pArgs[5]), Float.parseFloat(pArgs[6]), Integer.parseInt(pArgs[7]), pArgs[8], pArgs[9], Integer.parseInt(pArgs[10]));
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
