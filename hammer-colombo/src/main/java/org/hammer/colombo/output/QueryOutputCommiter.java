package org.hammer.colombo.output;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.hammer.colombo.utils.StatUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.hadoop.io.BSONWritable;

/**
 * 
 * Query output commiter
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Colombo
 *
 */
public class QueryOutputCommiter extends OutputCommitter {

	private static final Log LOG = LogFactory.getLog(QueryOutputCommiter.class);

	private int inserted = 0;
	private final DBCollection collection;
	public static final String TEMP_DIR_NAME = "_MONGO_OUT_TEMP";

	public QueryOutputCommiter(final DBCollection collection) {
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
		LOG.info("COLOMBO QUERY COMMITTER - Aborting task.");
		cleanupTemporaryFiles(taskContext);
	}

	@Override
	public void commitTask(final TaskAttemptContext taskContext) throws IOException {
		LOG.info("COLOMBO QUERY COMMITER - Committing task.");

		inserted = 0;
		Path tempFilePath = getTaskAttemptPath(taskContext);
		LOG.info("COLOMBO QUERY-  Committing from temporary file: " + tempFilePath.toString());

		FSDataInputStream inputStream = null;
		try {
			FileSystem fs = FileSystem.get(taskContext.getConfiguration());
			inputStream = fs.open(tempFilePath);
		} catch (IOException e) {
			LOG.error("COLOMBO QUERYCOMMITER  Could not open temporary file for committing", e);
			LOG.error(e);
			cleanupAfterCommit(inputStream, taskContext);

			throw e;
		}

		LOG.info("COLOMBO QUERY Clean collection " + collection.getFullName());

		collection.remove(new BasicDBObject());
		LOG.info("COLOMBO QUERY After clear : collection " + collection.count());

		inserted = 0;
		while (inputStream.available() > 0) {
			try {
				BSONWritable bw = new BSONWritable();
				bw.readFields(inputStream);
				BasicDBObject bo = new BasicDBObject(bw.getDoc().toMap());

				// SAVE LIST OF RESOURCE TO MONGO DB
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
				LOG.error("COLOMBO QUERY: Error reading from temporary file", e);
				throw new IOException(e);
			}
		}

		LOG.info("COLOMBO QUERY INSERT - DATA SET : " + inserted);



		
		// save the stat
		BSONObject statObj = new BasicBSONObject();
		statObj.put("type", "stat");
		statObj.put("record-total", 0);
		statObj.put("record-selected", 0);
		statObj.put("resource-count", inserted);
		statObj.put("size", 0);
		statObj.put("fuzzy-query", 0);
		statObj.put("total-query", 0);
		StatUtils.SaveStat(taskContext.getConfiguration(), statObj);

		cleanupAfterCommit(inputStream, taskContext);
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
				LOG.error("COLOMBO QUERY COMMITER  - Could not close input stream", e);
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
			LOG.error("COLOMBO QUERY COMMITER  - Could not open filesystem", e);
			throw e;
		}
	}

	@Override
	public void setupJob(final JobContext jobContext) {
		LOG.info("COLOMBO QUERY COMMITER - Setting up job.");
	}

	@Override
	public void setupTask(final TaskAttemptContext taskContext) {
		LOG.info("COLOMBO QUERY COMMITER - Setting up task.");



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