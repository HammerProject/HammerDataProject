package org.hammer.colombo.splitter;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;


/**
 * Query record reader
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Colombo
 *
 */
public class QueryRecordReader extends RecordReader<Object, BSONObject> {

	private static final Log LOG = LogFactory.getLog(QueryRecordReader.class);

	protected BSONObject current;
	protected final QuerySplit split;
	protected float seen = 0;
	protected float total = 0.0f;
	protected Configuration conf = null;

	public QueryRecordReader(final QuerySplit split) {
		this.split = split;
	}

	@Override
	public void close() {

	}

	@Override
	public Object getCurrentKey() {
		return split.getQueryString();
	}

	@Override
	public BSONObject getCurrentValue() {

		return current;
	}

	@Override
	public void initialize(final InputSplit split, final TaskAttemptContext context) {
		LOG.info("COLOMBO QUERY RECORD READER: Get QUERY");
		this.conf = context.getConfiguration();
		BasicBSONObject bObj = new BasicBSONObject();
		bObj.append("keywords", this.split.getKeywords());
		bObj.append("queryString", this.split.getQueryString());
		this.current = bObj;
		this.seen = 0;
		this.total = 1;

	}

	@Override
	public boolean nextKeyValue() {

		if (seen < total) {
			LOG.info("Read " + (seen + 1) + " query from (total " + total + ") :");
			LOG.info(" ----> " + this.getCurrentKey().toString());

			seen++;
			return true;
		}

		return false;
	}


	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (seen / total);
	}


}
