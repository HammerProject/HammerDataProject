package org.hammer.pinta;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hammer.pinta.output.PintaOutputCommiter;
import org.hammer.pinta.output.PintaRecordWriter;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * 
 * Pinta Output Format
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Pinta
 *
 */
public class PintaOutputFormat extends OutputFormat<Text, BSONWritable> {

    public void checkOutputSpecs(final JobContext context) throws IOException {
        if (MongoConfigUtil.getOutputURIs(context.getConfiguration()).isEmpty()) {
            throw new IOException("PINTA CONFIG ERROR: No output URI is specified. You must set mongo.output.uri.");
        }
    }

    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) {
        return new PintaOutputCommiter(
        		MongoConfigUtil.getOutputCollection(context.getConfiguration()));
    }

    /**
     * Get the record writer that points to the output collection.
     */
    public RecordWriter<Text, BSONWritable> getRecordWriter(final TaskAttemptContext context) {
        return new PintaRecordWriter(
          MongoConfigUtil.getOutputCollection(context.getConfiguration()),
          context);
    }

    public PintaOutputFormat() {}

   
    
    
}