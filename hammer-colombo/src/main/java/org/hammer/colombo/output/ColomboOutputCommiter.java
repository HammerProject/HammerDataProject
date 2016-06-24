package org.hammer.colombo.output;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
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
import com.mongodb.hadoop.io.BSONWritable;

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
	private float thSim = 0.0f;

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
				// we applicate all the and condition (for reduce data)
				// if we download data we don't apply the selection condition

				if (applyWhereCondition(bo)) {

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
				LOG.error("PINTA COMMITTER: Error reading from temporary file", e);
				throw new IOException(e);
			}
		}

		LOG.info("PINTA INSERT - DATA SET : " + inserted);

		cleanupAfterCommit(inputStream, taskContext);
	}

	/**
	 * Apply where condition
	 * 
	 * @param conf
	 * @param q
	 */
	private boolean applyWhereCondition(BasicDBObject bo) throws Exception {
		//
		// check AND condition
		//
		boolean check = true;
		int c = 0;
		for (Edge en : q.getQueryCondition()) {
			for (Node ch : en.getChild()) {

				if ((ch instanceof ValueNode) && en.getCondition().equals("and")) {
					c++;
				}
			}
		}
		for (Edge en : q.getQueryCondition()) {
			LOG.info(en.getCondition());
			LOG.info(en.getOperator());
			LOG.info("------------------------------------");

			for (Node ch : en.getChild()) {

				if ((ch instanceof ValueNode) && en.getCondition().equals("and")) {
					for (String column : bo.keySet()) {

						double sim = JaroWinkler.Apply(en.getName().toLowerCase(), column.toLowerCase());
						String value = bo.getString(column);

						LOG.info(en.getName().toLowerCase() + " -- " + column.toLowerCase());
						LOG.info(ch.getName().toLowerCase() + " -- " + value);

						if (sim >= thSim) {
							c--;
							if (en.getOperator().equals("eq") && !ch.getName().toLowerCase().equals(value)) {
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

		return (c == 0 && check);

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
		thSim = Float.parseFloat(conf.get("thSim"));
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