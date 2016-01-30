package org.hammer.pinta.splitter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;

/**
 * 
 * Pinta Split
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Pinta
 *
 */
public class PintaSplit extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit {
    	
    public PintaSplit() {
    }

    private String id = "";
    
    
    private List<BasicDBObject> dataset = new ArrayList<BasicDBObject>();
        
	protected transient BSONEncoder _bsonEncoder = new BasicBSONEncoder();
    protected transient BSONDecoder _bsonDecoder = new BasicBSONDecoder();


    public void write(final DataOutput out) throws IOException {
        BSONObject spec = BasicDBObjectBuilder.start()
                                              .add("id", getId())
                                              .add("dataset", getDataset() != null ? getDataset() : null)
                                              .get();
        byte[] buf = _bsonEncoder.encode(spec);
        out.write(buf);
    }


    @SuppressWarnings("unchecked")
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
        setId(spec.get("id").toString());
        setDataset((List<BasicDBObject>) spec.get("dataset"));
    }


    @Override
    public String toString() {
        return "DataSetSplit{ID=" + this.id.toString()
        	   + ", dataset n. =" + this.dataset.size() + '}';
    }

    @Override
    public int hashCode() {
        int result = this.id != null ? this.id.hashCode() : 0;
        for(BasicDBObject k : dataset) {
        	result = 31 * result + (k.hashCode());
        }
        return result;
    }

	@Override
	public long getLength() {
		return Long.MAX_VALUE;
	}

	@Override
	public String[] getLocations() {
		String[] urls = new String[1];
		urls[0] = this.id;
		return urls;
	}


	public String getId() {
		return id;
	}


	public void setId(String id) {
		this.id = id;
	}


	public List<BasicDBObject> getDataset() {
		return dataset;
	}


	public void setDataset(List<BasicDBObject> dataset) {
		this.dataset = dataset;
	}


}
