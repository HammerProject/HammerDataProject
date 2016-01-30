package org.hammer.santamaria.splitter;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;

/**
 * 
 * Santa Maria Record Reader - 
 * Base record reader for tester and data source without implementation
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Santa Maria
 *
 */
public class BaseDataSourceRecordReader extends RecordReader<Object, BSONObject> {

    public BaseDataSourceRecordReader(final DataSourceSplit split) {
        this.split = split;
        
    }

    @Override
    public void close() {
    	
    }

    @Override
    public Object getCurrentKey() {
        Object key = split.getName();
        return null != key ? key : NullWritable.get();
    }

    @Override
    public BSONObject getCurrentValue() {
        return current;
    }

    /**
     * Get Progess
     * from 0 to 1
     */
    public float getProgress() {
        return (seen/total);
    }

    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context) {
        total = (float) Math.floor((Math.random() * 10) + 1);
    }

    @Override
    public boolean nextKeyValue() {
    	LOG.info("Read " + seen + " documents from (total " + total + ") :");
    	LOG.info(split.getName());
    	if(seen < total) {
    		seen++;
    		return true;
    	}
    	return false;
    }


    protected BSONObject current;
    protected final DataSourceSplit split;
    protected float seen = 0;
    protected float total = 0.0f;

    private static final Log LOG = LogFactory.getLog(BaseDataSourceRecordReader.class);

}