package org.hammer.colombo.splitter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;

import com.mongodb.BasicDBObjectBuilder;

/**
 * 
 * Query Split
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Colombo
 *
 */
public class QuerySplit extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit {
	
	/**
	 * Build the query split
	 */
    public QuerySplit() {
    }
    
	/**
	 * Encoder
	 */
	protected transient BSONEncoder _bsonEncoder = new BasicBSONEncoder();
	
	/**
	 * Decoder
	 */
    protected transient BSONDecoder _bsonDecoder = new BasicBSONDecoder();

    /**
     * Query
     */
    private String queryString = "";
    
    /**
     * Get my query string
     * @return
     */
    public String getQueryString() {
		return queryString;
	}

    /**
     * Write to buffer
     */
    public void write(final DataOutput out) throws IOException {
        BSONObject spec = BasicDBObjectBuilder.start()
                                              .add("queryString", getQueryString())
                                              .get();
        byte[] buf = _bsonEncoder.encode(spec);
        out.write(buf);
    }

    /**
     * Read from buffer
     */
    public void readFields(final DataInput in) throws IOException {
        BSONCallback cb = new BasicBSONCallback();
        BSONObject spec;
        byte[] l = new byte[4];
        in.readFully(l);
        int dataLen = org.bson.io.Bits.readInt(l);
        byte[] data = new byte[dataLen + 4];
        System.arraycopy(l, 0, data, 0, 4);
        in.readFully(data, 4, dataLen - 4);
        _bsonDecoder.decode(data, cb);
        spec = (BSONObject) cb.get();
        setQueryString(spec.get("queryString").toString());
    }


    @Override
    public String toString() {
        return "QuerySplit{queryString=" + this.queryString + '}';
    }

    @Override
    public int hashCode() {
        int result = this.queryString != null ? this.queryString.hashCode() : 0;
        return result;
    }

	@Override
	public long getLength() {
		return Long.MAX_VALUE;
	}

	@Override
	public String[] getLocations() {
		String[] urls = new String[1];
		urls[0] = this.queryString;
		return urls;
	}


	/**
	 * Set my query string
	 * @param queryString
	 */
	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}


}
