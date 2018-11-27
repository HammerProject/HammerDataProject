package org.hammer.shark;

import java.io.IOException;
import java.io.StringReader;

import org.apache.spark.sql.SparkSession;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.cc.util.IsabellaUtils;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.QueryGraph;
import org.hammer.shark.engine.SharkResource;
import org.hammer.shark.query.SharkQuery2;
import org.hammer.shark.utils.Config;
import org.hammer.shark.utils.StatUtils;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

/**
 * Hello world!
 *
 */
public class App {

	/**
	 * 
	 * Run the Shark Search Engine
	 * 
	 * @param fileQuery
	 *            path to file
	 * @param fileSystem
	 *            hdfs or local
	 * @param searchMode
	 *            download or search
	 * @param queryMode
	 *            labels or keywords
	 * @param thKrm
	 *            the krm value to cut the resources (during relevant resource
	 *            search)
	 * @param thRm
	 *            the rm value to cut the resources (during sdf phase)
	 * @param thSim
	 *            the sim value to cut record during apply where condition
	 * @param maxSim
	 *            max number of sim
	 * @param datasetTable
	 *            the dataset table
	 * @param indexTable
	 *            the index table
	 * @param thQuery
	 *            the limit used to prunning the set of query (using cos(theta)
	 *            function)
	 * @throws Exception
	 */
	public static void Run(String fileQuery, String searchMode, String queryMode, float thKrm, float thRm, float thSim,
			int maxSim, String datasetTable, String indexTable, int thQuery, float cosSim) throws Exception {
		System.out.println("!!! Hammer Project !!!");
		System.out.println("!!! Shark Module start.....");

		SparkSession spark = SparkSession.builder().appName("SHARK").getOrCreate();
		if (!spark.sparkContext().isLocal()) {
			// send files to each worker!
			spark.sparkContext().addFile("shark.conf");

		}
		// init config
		Config.init("shark.conf");

		spark.sparkContext().conf().set("spark.mongodb.input.uri",
				Config.getInstance().getConfig().getString("spark.mongodb.input.uri"));
		spark.sparkContext().conf().set("spark.mongodb.output.uri",
				Config.getInstance().getConfig().getString("spark.mongodb.output.uri"));
		spark.sparkContext().setCheckpointDir("/home/hadoop/software/shark/checkpoint");

		spark.sparkContext().conf().set("spark.network.timeout ", "1200");
		spark.sparkContext().conf().set("spark.rpc.askTimeout ", "1200");

		String query = "";
		spark.sparkContext().conf().set("query-file", fileQuery);
		query = IsabellaUtils.readFile(fileQuery);

		// set the parameter
		spark.sparkContext().conf().set("search-mode", searchMode);
		spark.sparkContext().conf().set("query-mode", queryMode);
		spark.sparkContext().conf().set("query-string", query);
		spark.sparkContext().conf().set("thRm", thRm + "");
		spark.sparkContext().conf().set("thKrm", thKrm + "");
		spark.sparkContext().conf().set("thSim", thSim + "");
		spark.sparkContext().conf().set("maxSim", maxSim + "");
		spark.sparkContext().conf().set("dataset-table", datasetTable + "");
		spark.sparkContext().conf().set("index-table", indexTable + "");
		spark.sparkContext().conf().set("thQuery", thQuery + "");
		spark.sparkContext().conf().set("cosSim", cosSim + "");

		// check the query
		Isabella parser = new Isabella(new StringReader(query));
		QueryGraph q;
		try {
			q = parser.queryGraph();
		} catch (ParseException e) {
			throw new IOException(e);
		}
		q.setIndex(StatUtils.GetMyIndex());
		q.setWnHome(Config.getInstance().getConfig().getString("whHome"));

		for (IsabellaError err : parser.getErrors().values()) {
			System.out.println(err.toString());
		}
		if (parser.getErrors().size() > 0) {
			throw new IOException("Query syntax not correct.");
		}

		// the the paramter from the query
		spark.sparkContext().conf().set("query-table", "query" + (q.hashCode() + "").replaceAll("-", "_"));
		spark.sparkContext().conf().set("list-result", "list" + (q.hashCode() + "").replaceAll("-", "_"));
		spark.sparkContext().conf().set("resource-table", "resource" + (q.hashCode() + "").replaceAll("-", "_"));
		spark.sparkContext().conf().set("stat-result", "stat" + (q.hashCode() + "").replaceAll("-", "_"));
		spark.sparkContext().conf().set("joinCondition", q.getJoinCondition());

		System.out.println("******************************************************************");
		System.out.println("******************************************************************");
		System.out.println("******************************************************************");
		System.out.println("SHARK Create temp table " + spark.sparkContext().conf().get("query-table"));
		System.out.println("SHARK Create resources table " + spark.sparkContext().conf().get("resource-table"));
		System.out.println("SHARK Create list resources " + spark.sparkContext().conf().get("list-result"));
		System.out.println("SHARK Create stat output " + spark.sparkContext().conf().get("stat-result"));
		System.out.println("******************************************************************");
		System.out.println("******************************************************************");
		System.out.println("******************************************************************");

		// init table
		MongoClient mongo = null;
		MongoDatabase db = null;
		try {
			MongoClientURI outputURI = new MongoClientURI(
					Config.getInstance().getConfig().getString("spark.mongodb.output.uri"));
			mongo = new MongoClient(outputURI);
			db = mongo.getDatabase(outputURI.getDatabase());
			System.out.println("SHARK QUERY Create temp table " + spark.sparkContext().conf().get("query-table"));
			if (db.getCollection(spark.sparkContext().conf().get("query-table")) != null) {
				db.getCollection(spark.sparkContext().conf().get("query-table")).drop();
			}
			db.createCollection(spark.sparkContext().conf().get("query-table"));
			
			if (db.getCollection(spark.sparkContext().conf().get("resource-table")) != null) {
				db.getCollection(spark.sparkContext().conf().get("resource-table")).drop();
			}
			db.createCollection(spark.sparkContext().conf().get("resource-table"));
			
			if (db.getCollection(spark.sparkContext().conf().get("list-result")) != null) {
				db.getCollection(spark.sparkContext().conf().get("list-result")).drop();
			}
			db.createCollection(spark.sparkContext().conf().get("list-result"));
		
			if (db.getCollection(spark.sparkContext().conf().get("stat-result")) != null) {
				db.getCollection(spark.sparkContext().conf().get("stat-result")).drop();
			}
			db.createCollection(spark.sparkContext().conf().get("stat-result"));
			
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}
		// 1phase MapReduce --> calc the fuzzy-query list and populate the
		// resource-table
		
		SharkQuery2 SHARKQUERY = new SharkQuery2(spark);
		SHARKQUERY.calculateResources(spark);

		// 2phase MapReduce --> download the resource-table, apply the selection model
		// (with fuzzy logic)
		// and return the record in query-table
		
		
		
		SharkResource SHARKRESOURCE = new SharkResource(spark);
		SHARKRESOURCE.getItems(spark);

		spark.close();
	}

	public static void main(String[] pArgs) throws Exception {

		if (pArgs == null || pArgs.length < 10) {
			throw new Exception(
					"Parameter: <path_to_query> <search mode: search|download> <query mode: keywords|labels> <thKrm: 0.5|0.01..>  <thRm: 0.5|0.01..> <thSim: 0.5|0.01..> <maxSim: 1|2|3...> <dataset-table> <index-table> <queryLimit:10|100|1000> <cosTh:0.9991|0.9992>");
		}
		Run(pArgs[0], pArgs[1], pArgs[2], Float.parseFloat(pArgs[3]), Float.parseFloat(pArgs[4]),
				Float.parseFloat(pArgs[5]), Integer.parseInt(pArgs[6]), pArgs[7], pArgs[8], Integer.parseInt(pArgs[9]), Float.parseFloat(pArgs[10]));
	}
}
