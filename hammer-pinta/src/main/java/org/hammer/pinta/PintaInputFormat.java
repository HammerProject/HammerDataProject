package org.hammer.pinta;

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
import org.hammer.pinta.splitter.PintaRecordReader;
import org.hammer.pinta.splitter.PintaSplit;

import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.MongoSplitterFactory;
import com.mongodb.hadoop.splitter.SplitFailedException;

/**
 * 
 * Pinta Input Format
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Pinta
 *
 */
public class PintaInputFormat extends InputFormat<Object, BSONObject> {

    private static final Log LOG = LogFactory.getLog(PintaInputFormat.class);

	public RecordReader<Object, BSONObject> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        if (!(split instanceof PintaSplit)) {
            throw new IllegalStateException("Creation of a new RecordReader requires a PintaSplit instance.");
        }
        final PintaSplit mis = (PintaSplit) split;
        RecordReader<Object, BSONObject> t = new PintaRecordReader(mis);		
        return t;
    }

    public List<InputSplit> getSplits(final JobContext context) throws IOException {
    	System.out.println(context.getClass().toString());
        final Configuration conf = context.getConfiguration();



        try {
            MongoSplitter splitterImpl = MongoSplitterFactory.getSplitter(conf);
            LOG.debug("PINTA on MongoDB - Using " + splitterImpl.toString() + " to calculate splits.");
            return splitterImpl.calculateSplits();
        } catch (SplitFailedException spfe) {
            throw new IOException(spfe);
        }
    }

    public boolean verifyConfiguration(final Configuration conf) {
        return true;
    }
    
    


    
}