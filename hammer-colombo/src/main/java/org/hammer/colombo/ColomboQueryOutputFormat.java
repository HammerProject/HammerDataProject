package org.hammer.colombo;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hammer.colombo.output.QueryOutputCommiter;
import org.hammer.colombo.output.QueryRecordWriter;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * 
 * Colombo Query Output Format
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Colombo
 *
 */
public class ColomboQueryOutputFormat extends OutputFormat<Text, BSONWritable> {

    public void checkOutputSpecs(final JobContext context) throws IOException {
        if (MongoConfigUtil.getOutputURIs(context.getConfiguration()).isEmpty()) {
            throw new IOException("COLOMBO QUERY CONFIG ERROR: No output URI is specified. You must set mongo.output.uri.");
        }
    }

    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) {
        return new QueryOutputCommiter(
        		MongoConfigUtil.getOutputCollection(context.getConfiguration()));
    }

    /**
     * Get the record writer that points to the output collection.
     */
    public RecordWriter<Text, BSONWritable> getRecordWriter(final TaskAttemptContext context) {
        return new QueryRecordWriter(
          MongoConfigUtil.getOutputCollection(context.getConfiguration()),
          context);
    }

    public ColomboQueryOutputFormat() {}

   
    
    
}