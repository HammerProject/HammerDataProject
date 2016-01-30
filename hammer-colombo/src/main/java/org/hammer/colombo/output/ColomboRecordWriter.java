package org.hammer.colombo.output;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.output.MongoOutputCommitter;

/**
 * 
 * Pinta Record Writer
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Pinta
 *
 */
public class ColomboRecordWriter extends RecordWriter<Text, BSONWritable> {

	private static final Log LOG = LogFactory.getLog(ColomboRecordWriter.class);
	private final DBCollection collection;
	private final TaskAttemptContext context;
	private final BSONWritable bsonWritable;
	private FSDataOutputStream outputStream;

	/**
     * Create a MongoRecordWriter targeting a single DBCollection.
     * @param c a DBCollection
     * @param ctx the TaskAttemptContext
     */
    public ColomboRecordWriter(final DBCollection c, final TaskAttemptContext ctx) {
        collection = c;
        context = ctx;
        bsonWritable = new BSONWritable();
        try {
            FileSystem fs = FileSystem.get(ctx.getConfiguration());
            Path outputPath = MongoOutputCommitter.getTaskAttemptPath(ctx);
            LOG.info("PINTA Writing to temporary file: " + outputPath.toString());
            outputStream = fs.create(outputPath, true);
        } catch (IOException e) {
            LOG.error("PINTA  Could not open temporary file for buffering Mongo output", e);
        }
    }


	@Override
	public void close(final TaskAttemptContext context) {
		if (outputStream != null) {
			try {
				outputStream.close();
			} catch (IOException e) {
				LOG.error("Could not close output stream", e);
			}
		}
	}

	@Override
	public void write(final Text key, final BSONWritable value) throws IOException {
		DBObject o = new BasicDBObject();
		o.putAll(((BSONWritable) value).getDoc());
		o.put("_id", BSONWritable.toBSON(key));
		bsonWritable.setDoc(o);
		bsonWritable.write(outputStream);
		
	}

	/**
	 * Add an index to be ensured before the Job starts running.
	 * 
	 * @param index
	 *            a DBObject describing the keys of the index.
	 * @param options
	 *            a DBObject describing the options to apply when creating the
	 *            index.
	 */
	public void ensureIndex(final DBObject index, final DBObject options) {
		collection.createIndex(index, options);
	}

	/**
	 * Get the TaskAttemptContext associated with this MongoRecordWriter.
	 * 
	 * @return the TaskAttemptContext
	 */
	public TaskAttemptContext getContext() {
		return context;
	}

}
