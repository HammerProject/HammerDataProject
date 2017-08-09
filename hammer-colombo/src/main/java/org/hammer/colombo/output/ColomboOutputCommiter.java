package org.hammer.colombo.output;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.hammer.colombo.utils.StatUtils;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.fuzzy.JaroWinkler;
import org.hammer.isabella.query.Edge;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Keyword;
import org.hammer.isabella.query.Node;
import org.hammer.isabella.query.QueryGraph;
import org.hammer.isabella.query.ValueNode;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * 
 * Colombo output commiter
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - PINTA
 *
 */
public class ColomboOutputCommiter extends OutputCommitter {

	private static final Log LOG = LogFactory.getLog(ColomboOutputCommiter.class);

	private int inserted = 0;
	private QueryGraph q = null;
	private final DBCollection collection;
	public static final String TEMP_DIR_NAME = "_MONGO_OUT_TEMP";
	private double thSim = 0.0d;

	public ColomboOutputCommiter(final DBCollection collection) {
		this.collection = collection;
	}

	private void cleanupTemporaryFiles(final TaskAttemptContext taskContext) throws IOException {
		Path tempPath = getTaskAttemptPath(taskContext);
		try {
			FileSystem fs = FileSystem.get(taskContext.getConfiguration());
			fs.delete(tempPath, true);
		} catch (IOException e) {
			LOG.error("Could not delete temporary file " + tempPath, e);
			LOG.error(e);
			throw e;
		}
	}

	@Override
	public void abortTask(final TaskAttemptContext taskContext) throws IOException {
		LOG.info("COLOMBO COMMITTER - Aborting task.");
		cleanupTemporaryFiles(taskContext);
	}

	@Override
	public void commitTask(final TaskAttemptContext taskContext) throws IOException {
		LOG.info("COLOMBO COMMITER - Committing task.");

		inserted = 0;
		Path tempFilePath = getTaskAttemptPath(taskContext);
		LOG.info("COLOMBO -  Committing from temporary file: " + tempFilePath.toString());

		FSDataInputStream inputStream = null;
		try {
			FileSystem fs = FileSystem.get(taskContext.getConfiguration());
			inputStream = fs.open(tempFilePath);
		} catch (IOException e) {
			LOG.error("COLOMBO COMMITER  Could not open temporary file for committing", e);
			LOG.error(e);
			cleanupAfterCommit(inputStream, taskContext);

			throw e;
		}

		LOG.info("COLOMBO Clean collection " + collection.getFullName());

		collection.remove(new BasicDBObject());
		LOG.info("COLOMBO After clear : collection " + collection.count());

		inserted = 0;
		while (inputStream.available() > 0) {
			try {
				BSONWritable bw = new BSONWritable();
				bw.readFields(inputStream);
				BasicDBObject bo = new BasicDBObject(bw.getDoc().toMap());
				// we applicate all the condition (for reduce data)
				// if we download data we don't apply the selection condition

				if (applyWhereCondition(bo, taskContext.getConfiguration())) {

					BasicDBObject searchQuery = new BasicDBObject().append("_id", bo.get("_id"));
					DBCursor c = collection.find(searchQuery);
					if (!c.hasNext()) {
						collection.insert(bo);
						inserted++;
					}
					c.close();
				}
				taskContext.progress();

			} catch (Exception e) {
				LOG.debug(e);
				LOG.error("COLOMBO COMMITTER: Error reading from temporary file", e);
				throw new IOException(e);
			}
		}

		LOG.info("COLOMBO INSERT - DATA SET : " + inserted);

		cleanupAfterCommit(inputStream, taskContext);
	}

	private Map<String, List<String>> synset = new HashMap<String, List<String>>();
	
	
	/**
	 * Verify if field is in synset of column
	 * 
	 * @param column the column
	 * @param field the field
	 * @param conf the configuration for access hadoop
	 * @return true or false
	 */
	private boolean checkSynset(String column, String field, Configuration conf) {
		
		if(synset.containsKey(column)) {
			List<String> mySynSet = synset.get(column.toLowerCase());
			return mySynSet.contains(field.toLowerCase());
		}
		
		
		boolean check = false;
		MongoClient mongo = null;
		MongoDatabase db = null;
		try {
			MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());
			MongoCollection<Document> myIdx = db.getCollection(conf.get("index-table") + "");
			BasicDBObject searchQuery = new BasicDBObject().append("keyword", column.toLowerCase());
			FindIterable<Document> myTerm = myIdx.find(searchQuery);
			if (myTerm.iterator().hasNext()) {
				Document obj = myTerm.iterator().next();
				@SuppressWarnings("unchecked")
				ArrayList<Document> dbSynSet = (ArrayList<Document>) obj.get("syn-set");
				ArrayList<String> mySynSet = new ArrayList<String>();
				if (dbSynSet != null) {
					for(Document o: dbSynSet) {
						mySynSet.add((o.get("term") + "").toLowerCase());
					}
				}
				synset.put(column.toLowerCase(), mySynSet);
				
				
			}
			
			
			if(synset.containsKey(column)) {
				List<String> mySynSet = synset.get(column.toLowerCase());
				check = mySynSet.contains(field.toLowerCase());
			}
			
		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
			LOG.error(ex.getMessage());
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}
		
		return check;
	}
	
	/**
	 * Apply where condition
	 * 
	 * @param conf
	 * @param q
	 */
	private boolean applyWhereCondition(BasicDBObject bo, Configuration conf) throws Exception {
		//
		// check OR condition
		//
		/*
		 
		boolean orCheck = false;
		int orCount = 0;
		for (Edge en : q.getQueryCondition()) {
			LOG.info(en.getCondition());
			LOG.info(en.getOperator());
			LOG.info("------------------------------------");
			for (Node ch : en.getChild()) {

				

				if ((ch instanceof ValueNode) && en.getCondition().equals("or")) {
					orCount ++;
					
					for (String column : bo.keySet()) {
						String value = bo.getString(column);
						
						LOG.info(en.getName().toLowerCase() + " -- " + column.toLowerCase());
						LOG.info(ch.getName().toLowerCase() + " -- " + value);
						
						boolean syn = checkSynset(en.getName().toLowerCase(), column.toLowerCase(), conf);
						double sim = JaroWinkler.Apply(en.getName().toLowerCase(), column.toLowerCase());

						LOG.info("test " + sim + ">=" + thSim + " -- syn: " + syn);
						if ((sim >= thSim)||(syn)) {
							LOG.info("ok sim --> " + sim);
							LOG.info("ok syn --> " + syn);

							LOG.info("check  --> " + ch.getName().toLowerCase().compareTo(value));
							double simV = JaroWinkler.Apply(ch.getName().toLowerCase(), value.toLowerCase());
							LOG.info("check  --> " + simV);
							if (en.getOperator().equals("eq")
									&& (ch.getName().toLowerCase().equals(value) || (simV >= thSim))) {
								orCheck = true;
							} else if (en.getOperator().equals("gt")) {
								if (ch.getName().toLowerCase().compareTo(value) > 0) {
									orCheck = true;
								}
							} else if (en.getOperator().equals("lt")) {
								if (ch.getName().toLowerCase().compareTo(value) < 0) {
									orCheck = true;
								}
							} else if (en.getOperator().equals("ge")) {
								if (ch.getName().toLowerCase().compareTo(value) >= 0) {
									orCheck = true;
								}
							} else if (en.getOperator().equals("le")) {
								if (ch.getName().toLowerCase().compareTo(value) <= 0) {
									orCheck = true;
								}
							} else if (en.getName().equals(column)) {
								if (ch.getName().toLowerCase().compareTo(value) == 0) {
									orCheck = true;
								}
							}
						}

					}
				}

			}
		}

		LOG.info("---> " + orCheck);

		*/
		
		//
		// check AND condition
		//
		boolean check = true;
		int c = 0;
		List <String> andColumn = new ArrayList<String>();
		for (Edge en : q.getQueryCondition()) {
			for (Node ch : en.getChild()) {

				if ((ch instanceof ValueNode) && en.getCondition().equals("and")) {
					c++;
					// calc the best similarity column
					double maxSim = 0.0d;
					String myColumn = "";
					for (String column : bo.keySet()) {
						boolean syn = checkSynset(en.getName().toLowerCase(), column.toLowerCase(), conf);
						double sim = JaroWinkler.Apply(en.getName().toLowerCase(), column.toLowerCase());
						if((sim >= maxSim) || syn) {
							myColumn = column;
							maxSim = sim;
						}
						
						
					}
					andColumn.add(myColumn);
				}
			}
		}
		for (Edge en : q.getQueryCondition()) {
			LOG.info(en.getCondition());
			LOG.info(en.getOperator());
			LOG.info("------------------------------------");

			for (Node ch : en.getChild()) {

				if ((ch instanceof ValueNode) && en.getCondition().equals("and")) {
					for (String column : andColumn) {

						boolean syn = checkSynset(en.getName().toLowerCase(), column.toLowerCase(), conf);
						double sim = JaroWinkler.Apply(en.getName().toLowerCase(), column.toLowerCase());
						String value = bo.getString(column).toLowerCase();

						LOG.info(en.getName().toLowerCase() + " -- " + column.toLowerCase());
						LOG.info(ch.getName().toLowerCase() + " -- " + value);

						LOG.info("test " + sim + ">=" + thSim + " -- syn: " + syn);
						if ((sim >= thSim)||(syn)) {
							LOG.info("ok sim --> " + sim);
							LOG.info("ok syn --> " + syn);
							
							//c--;
							double simV = JaroWinkler.Apply(ch.getName().toLowerCase(), value.toLowerCase());
							LOG.info("check  --> " + simV);
							if (en.getOperator().equals("eq") && !ch.getName().toLowerCase().equals(value)
									&& (simV > thSim)) {
								check = false;
							} else if (en.getOperator().equals("gt")) {
								if (ch.getName().toLowerCase().compareTo(value) <= 0) {
									check = false;
								}
							} else if (en.getOperator().equals("lt")) {
								if (ch.getName().toLowerCase().compareTo(value) >= 0) {
									check = false;
								}
							} else if (en.getOperator().equals("ge")) {
								if (ch.getName().toLowerCase().compareTo(value) < 0) {
									check = false;
								}
							} else if (en.getOperator().equals("le")) {
								if (ch.getName().toLowerCase().compareTo(value) > 0) {
									check = false;
								}
							} else if (en.getName().equals(column)) {
								if (ch.getName().toLowerCase().compareTo(value) != 0) {
									check = false;
								}
							}
						}
					}

				}
			}
		}

		LOG.info("------------------------------------> check and condition " + c + " - " + check + " - " + (c == 0 || check));

		
		return (c == 0 || check) /* && (orCheck || orCount == 0)) */ ;

	}

	/**
	 * Helper method to close an FSDataInputStream and clean up any files still
	 * left around from map/reduce tasks.
	 * 
	 * @param inputStream
	 *            the FSDataInputStream to close.
	 */
	private void cleanupAfterCommit(final FSDataInputStream inputStream, final TaskAttemptContext context)
			throws IOException {
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (IOException e) {
				LOG.error(e);
				LOG.error("COLOMBO COMMITER  - Could not close input stream", e);
				throw e;
			}
		}
		cleanupTemporaryFiles(context);
	}

	@Override
	public boolean needsTaskCommit(final TaskAttemptContext taskContext) throws IOException {
		try {
			FileSystem fs = FileSystem.get(taskContext.getConfiguration());
			// Commit is only necessary if there was any output.
			return fs.exists(getTaskAttemptPath(taskContext));
		} catch (IOException e) {
			LOG.error(e);
			LOG.error("COLOMBO COMMITER  - Could not open filesystem", e);
			throw e;
		}
	}

	@Override
	public void setupJob(final JobContext jobContext) {
		LOG.info("COLOMBO COMMITER - Setting up job.");
	}

	@Override
	public void setupTask(final TaskAttemptContext taskContext) {
		LOG.info("COLOMBO COMMITER - Setting up task.");

		// first recreate the query
		Configuration conf = taskContext.getConfiguration();
		thSim = Precision.round(Double.parseDouble(conf.get("thSim")), 2);
		final HashMap<String, Keyword> kwIndex = StatUtils.GetMyIndex(conf);
		Isabella parser = new Isabella(new StringReader(conf.get("query-string")));
		try {
			q = parser.queryGraph();
			q.setIndex(kwIndex);
		} catch (ParseException e) {
			LOG.error(e);
		}
		for (IsabellaError err : parser.getErrors().values()) {
			LOG.error(err.toString());
		}
		//

	}

	/**
	 * Get the Path to where temporary files should be stored for a TaskAttempt,
	 * whose TaskAttemptContext is provided.
	 *
	 * @param context
	 *            the TaskAttemptContext.
	 * @return the Path to the temporary file for the TaskAttempt.
	 */
	public static Path getTaskAttemptPath(final TaskAttemptContext context) {
		Configuration config = context.getConfiguration();
		// Try to use the following base temporary directories, in this order:
		// 1. New-style option for task tmp dir
		// 2. Old-style option for task tmp dir
		// 3. Hadoop system-wide tmp dir
		// 4. /tmp
		String basePath = config.get("mapreduce.task.tmp.dir",
				config.get("mapred.child.tmp", config.get("hadoop.tmp.dir", "/tmp")));
		// Hadoop Paths always use "/" as a directory separator.
		return new Path(String.format("%s/%s/%s/_out", basePath, context.getTaskAttemptID().toString(), TEMP_DIR_NAME));
	}

}