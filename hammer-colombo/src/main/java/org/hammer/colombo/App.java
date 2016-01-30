package org.hammer.colombo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.Document;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.cc.util.IsabellaUtils;
import org.hammer.isabella.query.Edge;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Node;
import org.hammer.isabella.query.QueryGraph;
import org.hammer.isabella.query.ValueNode;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.util.MongoConfigUtil;


/**
 * Hello world!
 * 
 * 
 *
 */
public class App {

	/**
	 * Encoder
	 */
	protected transient BSONEncoder _bsonEncoder = new BasicBSONEncoder();
	
	/**
	 * Decoder
	 */
    protected transient BSONDecoder _bsonDecoder = new BasicBSONDecoder();

    
	public static void Run(String fileQuery, String mode, float limit) throws Exception {
		System.out.println("!!! Hammer Project !!!");
		System.out.println("!!! Colombo Module start.....");
		Configuration conf = new Configuration();
		conf.set("thesaurus.url", "http://thesaurus.altervista.org/thesaurus/v1");
		
		
		conf.set("thesaurus.key", "bVKAPIcUum3hEFGKEBAu"); // x hammerproject

		conf.set("thesaurus.lang", "it_IT");
		
		// insert a limit to socrata recordset for memory head problem
		conf.set("socrata.record.limit", "30000");
		
		conf.set("mongo.splitter.class", "org.hammer.colombo.splitter.DataSetSplitter");
		conf.set("limit", limit + "");
		
		String query =  "";
		if(mode.equals("hdfs")) {
			conf.set("query-file", "hdfs://192.168.56.90:9000/hammer/" + fileQuery);
			query = ReadFileFromHdfs(conf);
		} else {
			conf.set("query-file", fileQuery);
			query = IsabellaUtils.readFile(fileQuery);
		}
		conf.set("query-string", query);
		System.out.println(query);
		Isabella parser = new Isabella(new StringReader(query));
		QueryGraph q;
		try {
			q = parser.queryGraph();
		} catch (ParseException e) {
			throw new IOException(e);
		}

		
		
		for (IsabellaError err : parser.getErrors().values()) {
			System.out.println(err.toString());
		}
		
		if(parser.getErrors().size() > 0) {
			throw new IOException("Query syntax not correct.");
		}

		q.test();
		q.labelSelection();

		conf.set("query-table", "query" + (q.hashCode() + "").replaceAll("-", "_"));
		conf.set("query-result", "result" + (q.hashCode() + "").replaceAll("-", "_"));
		conf.set("list-result", "list" + (q.hashCode() + "").replaceAll("-", "_"));
		//conf.set("query-out", "hdfs://192.168.56.90:9000/hammer/out" + (q.hashCode() + "").replaceAll("-", "_") + ".json");
		System.out.println("COLOMBO Create temp table " + (q.hashCode() + "").replaceAll("-", "_"));
		conf.set("keywords", q.getKeyWords());
		conf.set("joinCondition", q.getJoinCondition());
		
		
    
		
        
		ToolRunner.run(conf, new ColomboConfig(conf), new String[0]);
		
		QueryDb(conf, q);
		
		System.out.println("************************************************");
		System.out.println("check table:                                    ");
		System.out.println(conf.get("query-result"));
		System.out.println("************************************************");

	}
	
	public static void main(String[] pArgs) throws Exception {


		
		if(pArgs == null || pArgs.length < 3) {
			throw new Exception("Parameter: <path_to_query> <mode: local|hdfs> <limit: 0.5|0.01..>");
		}
		Run(pArgs[0], pArgs[1], Float.parseFloat(pArgs[2]));
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

    /**
	 * Query over Mongo Db
	 * 
	 * @param q
	 * @param queryTable
	 */
	public static void QueryDb(Configuration conf, QueryGraph q) {
		MongoClient mongo = null;
		final Map<String, Document> dataMap = new HashMap<String, Document>();
		MongoDatabase db = null;
		System.out.println("Colombo launch selection...");
		FileSystem fs = null;
		FSDataOutputStream fin = null;


		try {
			

			MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			MongoCollection<Document> queryTable = db.getCollection(conf.get("query-table"));

			if(db.getCollection(conf.get("query-result")) == null) {
					db.createCollection(conf.get("query-result"));
			}
			db.getCollection(conf.get("query-result")).deleteMany(new BasicDBObject());
			System.out.println("COLOMBO After clear result : collection " + db.getCollection(conf.get("query-result")).count());
			

			// create or condition
			BasicDBList wList = new BasicDBList();

			for (Edge en : q.getQueryCondition()) {
				for (Node ch : en.getChild()) {
					if ((ch instanceof ValueNode) && en.getCondition().equals("OR")) {
						BasicDBObject temp = null;
						if(en.getOperator().equals("=")) {
							temp = new BasicDBObject(en.getName(), new BasicDBObject("$regex", ch.getName()));
						} else if (en.getOperator().equals(">")) {
							temp = new BasicDBObject(en.getName(), new BasicDBObject("$gt", ch.getName()));
						} else if (en.getOperator().equals("<")) {
							temp = new BasicDBObject(en.getName(), new BasicDBObject("$lt", ch.getName()));
						} else {
							temp = new BasicDBObject(en.getName(), new BasicDBObject("$regex", ch.getName()));
						}
						
						wList.add(temp);
					}
				}
			}

			
			BasicDBObject searchQuery = new BasicDBObject();
			
			if(wList.size() > 0) {
				searchQuery = new BasicDBObject("$or", wList);
			}
			
			//create and condition
			for (Edge en : q.getQueryCondition()) {
				for (Node ch : en.getChild()) {
					if ((ch instanceof ValueNode) && en.getCondition().equals("AND")) {
						if(en.getOperator().equals("=")) {
							searchQuery.append(en.getName(), new BasicDBObject("$regex", ch.getName()));
						} else if (en.getOperator().equals(">")) {
							searchQuery.append(en.getName(), new BasicDBObject("$gt", ch.getName()));
						} else if (en.getOperator().equals("<")) {
							searchQuery.append(en.getName(), new BasicDBObject("$lt", ch.getName()));
						} else {
							searchQuery.append(en.getName(), new BasicDBObject("$regex", ch.getName()));
						}
						
					}
				}
			}
			
			//create field select
			for (Edge qN : q.getQuestionNode()) {
				searchQuery.append(qN.getName(), true);
			}
			
			
			FindIterable<Document> iterable = queryTable.find(searchQuery);
			
			System.out.println("Colombo gets data from database..." + searchQuery.toString());

			

			iterable.forEach(new Block<Document>() {

				public void apply(final Document document) {
					dataMap.put(document.getString("_id"), document);
				}
			});

			System.out.println("Colombo find " + dataMap.size() + " record");

			/*
			fs = FileSystem.get(conf);
			Path outputPath = new Path(conf.get("query-out"));
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			fin = fs.create(outputPath);
			fin.write(new String("[").getBytes());
			int count = 0;
			for(Document d : dataMap.values()) {
				fin.write(d.toJson().getBytes());
				count ++;
				if(count != dataMap.size()) {
					fin.write(new String(",").getBytes());
				}
				db.getCollection(conf.get("query-result")).insertOne(d);
			}
			fin.write(new String("]").getBytes());
			*/
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				mongo.close();
			}
			if (fin != null) {
				try {
					fin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}

}
