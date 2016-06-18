package org.hammer.colombo.output;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.hammer.colombo.utils.StatUtils;
import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.query.Edge;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Keyword;
import org.hammer.isabella.query.Node;
import org.hammer.isabella.query.QueryGraph;
import org.hammer.isabella.query.ValueNode;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
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
 * PINTA output commiter
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - PINTA
 *
 */
public class ColomboOutputCommiter extends OutputCommitter {

	private static final Log LOG = LogFactory.getLog(ColomboOutputCommiter.class);

	private int inserted = 0;

	private final DBCollection collection;
	public static final String TEMP_DIR_NAME = "_MONGO_OUT_TEMP";

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
		LOG.info("PINTA COMMITTER - Aborting task.");
		cleanupTemporaryFiles(taskContext);
	}

	@Override
	public void commitTask(final TaskAttemptContext taskContext) throws IOException {
		LOG.info("PINTA COMMITER - Committing task.");

		inserted = 0;
		Path tempFilePath = getTaskAttemptPath(taskContext);
		LOG.info("PINTA -  Committing from temporary file: " + tempFilePath.toString());

		FSDataInputStream inputStream = null;
		try {
			FileSystem fs = FileSystem.get(taskContext.getConfiguration());
			inputStream = fs.open(tempFilePath);
		} catch (IOException e) {
			LOG.error("PINTA COMMITER  Could not open temporary file for committing", e);
			LOG.error(e);
			cleanupAfterCommit(inputStream, taskContext);

			throw e;
		}

		LOG.info("PINTA Clean collection " + collection.getFullName());

		collection.remove(new BasicDBObject());
		LOG.info("PINTA After clear : collection " + collection.count());

		inserted = 0;
		while (inputStream.available() > 0) {
			try {
				BSONWritable bw = new BSONWritable();
				bw.readFields(inputStream);
				BasicDBObject bo = new BasicDBObject(bw.getDoc().toMap());

				BasicDBObject searchQuery = new BasicDBObject().append("_id", bo.get("_id"));
				DBCursor c = collection.find(searchQuery);
				if (!c.hasNext()) {
					collection.insert(bo);
					inserted++;
				}
				c.close();

				taskContext.progress();

			} catch (Exception e) {
				LOG.debug(e);
				LOG.error("PINTA COMMITTER: Error reading from temporary file", e);
				throw new IOException(e);
			}
		}

		LOG.info("PINTA INSERT - DATA SET : " + inserted);

		// after insert we applicate we and condition (for reduce data)
		// if we download data we don't apply the selection condition
		if (taskContext.getConfiguration().get("search-mode").equals("download")) {
			try {
				this.applyWhereCondition(taskContext);
			} catch (Exception e) {
				LOG.error(e.getMessage());
				LOG.debug(e);
				throw new IOException(e);
			}
		}
		cleanupAfterCommit(inputStream, taskContext);
	}

	/**
	 * Apply where condition
	 * 
	 * @param conf
	 * @param q
	 */
	private void applyWhereCondition(final TaskAttemptContext taskContext) throws Exception {
		// first recreate the query
		Configuration conf = taskContext.getConfiguration();
		final HashMap<String, Keyword> kwIndex = StatUtils.GetMyIndex(conf);
		QueryGraph q = null;
		Isabella parser = new Isabella(new StringReader(conf.get("query-string")));
		try {
			q = parser.queryGraph();
			q.setIndex(kwIndex);
		} catch (ParseException e) {
			throw new Exception(e.getMessage());
		}
		for (IsabellaError err : parser.getErrors().values()) {
			LOG.error(err.toString());
		}
		//

		MongoClient mongo = null;
		final Map<String, Document> dataMap = new HashMap<String, Document>();
		MongoDatabase db = null;
		LOG.info("--- COLOMBO OUTPUT LAUNCH SELECTION PHASE ---");
		FileSystem fs = null;
		FSDataOutputStream fin = null;

		try {

			MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			MongoCollection<Document> queryTable = db.getCollection(conf.get("query-table"));

			if (db.getCollection(conf.get("query-result")) == null) {
				db.createCollection(conf.get("query-result"));
			}
			db.getCollection(conf.get("query-result")).deleteMany(new BasicDBObject());
			//
			// don't create or condition because reduce phase
			// has also check this condition
			//
			BasicDBObject searchQuery = new BasicDBObject();

			//
			// create AND condition
			//
			for (Edge en : q.getQueryCondition()) {
				for (Node ch : en.getChild()) {
					if ((ch instanceof ValueNode) && en.getCondition().equals("AND")) {
						if (en.getOperator().equals("=")) {
							searchQuery.append(en.getName(), new BasicDBObject("$regex", ch.getName()));
						} else if (en.getOperator().equals(">")) {
							searchQuery.append(en.getName(), new BasicDBObject("$gt", ch.getName()));
						} else if (en.getOperator().equals(">=")) {
							searchQuery.append(en.getName(), new BasicDBObject("$ge", ch.getName()));
						} else if (en.getOperator().equals("<")) {
							searchQuery.append(en.getName(), new BasicDBObject("$lt", ch.getName()));
						} else if (en.getOperator().equals("<=")) {
							searchQuery.append(en.getName(), new BasicDBObject("$le", ch.getName()));
						} else {
							searchQuery.append(en.getName(), new BasicDBObject("$regex", ch.getName()));
						}

					}
				}
			}

			FindIterable<Document> iterable = queryTable.find(searchQuery);

			LOG.info("FINAL QUERY FOR AND SELECTION..." + searchQuery.toString());

			iterable.forEach(new Block<Document>() {

				public void apply(final Document document) {
					dataMap.put(document.getString("_id"), document);
				}
			});

			for (Document d : dataMap.values()) {
				db.getCollection(conf.get("query-result")).insertOne(d);
			}
			LOG.info("--- COLOMBO FINAL FIND " + dataMap.size() + " RECORD");
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