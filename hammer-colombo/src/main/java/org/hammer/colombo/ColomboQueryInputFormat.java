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
import org.hammer.colombo.splitter.QueryRecordReader;
import org.hammer.colombo.splitter.QuerySplit;
import org.hammer.colombo.splitter.QuerySplitter;

import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.SplitFailedException;

/**
 * 
 * Colombo Query Input Format
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Colombo
 *
 */
public class ColomboQueryInputFormat extends InputFormat<Object, BSONObject> {

    private static final Log LOG = LogFactory.getLog(ColomboQueryInputFormat.class);

	public RecordReader<Object, BSONObject> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        if (!(split instanceof QuerySplit)) {
            throw new IllegalStateException("Creation of a new RecordReader requires a QuerySplit instance.");
        }
        System.out.println("COLOMBO get query for " + ((QuerySplit) split).getKeywords());
        final QuerySplit mis = (QuerySplit) split;
        RecordReader<Object, BSONObject> t = new QueryRecordReader(mis);		
        return t;
    }

    public List<InputSplit> getSplits(final JobContext context) throws IOException {
    	System.out.println(context.getClass().toString());
        try {
            MongoSplitter splitterImpl = new QuerySplitter(context.getConfiguration());
            LOG.debug("COLOMBO on MongoDB - Using " + splitterImpl.toString() + " to calculate query splits.");
            return splitterImpl.calculateSplits();
        } catch (SplitFailedException spfe) {
            throw new IOException(spfe);
        }
    }

    public boolean verifyConfiguration(final Configuration conf) {
        return true;
    }
    
    


    
}