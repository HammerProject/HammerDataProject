package org.hammer.pinta.splitter;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;


/**
 * Pinta record reader
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Pinta
 *
 */
public class PintaRecordReader extends RecordReader<Object, BSONObject> {

	private static final Log LOG = LogFactory.getLog(PintaRecordReader.class);


    protected BSONObject current;
    protected final PintaSplit split;
    protected int seen = 0;
    protected int total = 0;

	public PintaRecordReader(final PintaSplit split) {
		this.split = split;
	}

	@Override
	public void close() {

	}

	@Override
	public Object getCurrentKey() {
		return this.current.get("document");
	}

	@Override
	public BSONObject getCurrentValue() {
		return current;
	}

	@Override
	public void initialize(final InputSplit split, final TaskAttemptContext context) {
		LOG.info("PINTA RECORD READER: Initialize");
		this.seen = 0;
		this.current =  this.split.getDataset().get(0);
		this.total = this.split.getDataset().size();
		LOG.info("PINTA RECORD READER: total " + this.total);
	}

	@Override
	public boolean nextKeyValue() {

		if (seen < total) {
			LOG.info("Read " + (seen + 1) + " documents from (total " + total + ") :");
			
			this.current = this.split.getDataset().get(seen);
			LOG.info(this.current.toString());
			LOG.info(" ----> " + this.getCurrentKey().toString());
			seen++;
			return true;
		}

		return false;
	}


	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (seen/total);
	}
	
}
