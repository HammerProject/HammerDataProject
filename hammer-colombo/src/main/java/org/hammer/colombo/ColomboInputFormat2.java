package org.hammer.colombo;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;
import org.hammer.colombo.splitter.ColomboRecordReader;
import org.hammer.colombo.splitter.DataSetSplit;
import org.hammer.colombo.splitter.DataSetSplitter2;

import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.SplitFailedException;

/**
 * 
 * Colombo Input Format 2 version
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Colombo
 *
 */
public class ColomboInputFormat2 extends InputFormat<Object, BSONObject> {

    private static final Log LOG = LogFactory.getLog(ColomboInputFormat2.class);

	public RecordReader<Object, BSONObject> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        if (!(split instanceof DataSetSplit)) {
            throw new IllegalStateException("Creation of a new RecordReader requires a DataSetSplit instance.");
        }
        LOG.info("COLOMBO get record for " + ((DataSetSplit) split).getUrl());
        final DataSetSplit mis = (DataSetSplit) split;
        RecordReader<Object, BSONObject> t = new ColomboRecordReader(mis);		
        return t;
    }

    public List<InputSplit> getSplits(final JobContext context) throws IOException {
    	LOG.debug(context.getClass().toString());
        try {
            MongoSplitter splitterImpl = new DataSetSplitter2(context.getConfiguration());
            LOG.debug("COLOMBO on MongoDB - Using " + splitterImpl.toString() + " to calculate splits.");
            return splitterImpl.calculateSplits();
        } catch (SplitFailedException spfe) {
            throw new IOException(spfe);
        }
    }

    public boolean verifyConfiguration(final Configuration conf) {
        return true;
    }
    
    


    
}