package org.hammer.santamaria;

import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.MongoSplitterFactory;
import com.mongodb.hadoop.splitter.SplitFailedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;
import org.hammer.santamaria.splitter.DataSourceSplit;

import java.io.IOException;
import java.util.List;

/**
 * 
 * Santa Maria Input Format
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Santa Maria
 *
 */
public class SantaMariaInputFormat extends InputFormat<Object, BSONObject> {

    private static final Log LOG = LogFactory.getLog(SantaMariaInputFormat.class);


    @SuppressWarnings("unchecked")
	public RecordReader<Object, BSONObject> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        if (!(split instanceof DataSourceSplit)) {
            throw new IllegalStateException("Creation of a new RecordReader requires a DataSourceSplit instance.");
        }
        System.out.println("SANTA MARIA get record for " + ((DataSourceSplit) split).getUrl());
        final DataSourceSplit mis = (DataSourceSplit) split;
        System.out.println("SANTA MARIA get record for " + mis.getType());
        
        Class<?> c;
        RecordReader<Object, BSONObject> t;
		try {
			// TODO IMPLEMENTARE ANCHE LETTORE DA REGIONE LOMBARDIA
			c = Class.forName(mis.getType());
			c.getDeclaredConstructors();
			t = (RecordReader<Object, BSONObject>) c.getConstructor(DataSourceSplit.class).newInstance(mis);
	        
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException("Creation of a new RecordReader error: " + e.toString());
		}
	    
        return t;
    }

    public List<InputSplit> getSplits(final JobContext context) throws IOException {
    	System.out.println(context.getClass().toString());
        final Configuration conf = context.getConfiguration();
        try {
            MongoSplitter splitterImpl = MongoSplitterFactory.getSplitter(conf);
            LOG.debug("SANTA MARIA on MongoDB - Using " + splitterImpl.toString() + " to calculate splits.");
            return splitterImpl.calculateSplits();
        } catch (SplitFailedException spfe) {
            throw new IOException(spfe);
        }
    }

    public boolean verifyConfiguration(final Configuration conf) {
        return true;
    }
    
}