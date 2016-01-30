package org.hammer.santamaria;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hammer.santamaria.output.SantaMariaOutputCommiter;
import org.hammer.santamaria.output.SantaMariaRecordWriter;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * 
 * Santa Maria Output Format
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Santa Maria
 *
 */
public class SantaMariaOutputFormat extends OutputFormat<Text, BSONWritable> {

    public void checkOutputSpecs(final JobContext context) throws IOException {
        if (MongoConfigUtil.getOutputURIs(context.getConfiguration()).isEmpty()) {
            throw new IOException("SANTA MARIA CONFIG ERROR: No output URI is specified. You must set mongo.output.uri.");
        }
    }

    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) {
        return new SantaMariaOutputCommiter(
        		MongoConfigUtil.getOutputCollection(context.getConfiguration()));
    }

    /**
     * Get the record writer that points to the output collection.
     */
    public RecordWriter<Text, BSONWritable> getRecordWriter(final TaskAttemptContext context) {
        return new SantaMariaRecordWriter(
          MongoConfigUtil.getOutputCollection(context.getConfiguration()),
          context);
    }

    public SantaMariaOutputFormat() {}

   
    
    
}