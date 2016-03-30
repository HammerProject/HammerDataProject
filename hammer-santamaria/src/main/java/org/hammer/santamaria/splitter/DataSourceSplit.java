package org.hammer.santamaria.splitter;

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
 * Data Source Split
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Santa Maria
 *
 */
public class DataSourceSplit extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit {
    
	public static final String DEFAULT_RECORD_READER = "org.hammer.santamaria.splitter.BaseDataSourceRecordReader";
	
    public DataSourceSplit() {
    }

    private String type = "";
    
    
    private String url = "";
    
    private String action = "";
    
    public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	private String name = "";
    
	protected transient BSONEncoder _bsonEncoder = new BasicBSONEncoder();
    protected transient BSONDecoder _bsonDecoder = new BasicBSONDecoder();


    public void write(final DataOutput out) throws IOException {
        BSONObject spec = BasicDBObjectBuilder.start()
                                              .add("name", getName())
                                              .add("url", getUrl() != null ? getUrl() : null)
                                              .add("action", getAction() != null ? getAction() : null)
                                              .add("type", getType() != null ? getType() : DEFAULT_RECORD_READER)
                                              .get();
        byte[] buf = _bsonEncoder.encode(spec);
        out.write(buf);
    }


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
        setName(spec.get("name").toString());
        setUrl(spec.get("url").toString());
        setAction(spec.get("action").toString());
        setType(spec.get("type").toString());
    }


    @Override
    public String toString() {
        return "DataSourceSplit{URL=" + this.url.toString()
        	   + ", action=" + this.action
               + ", name=" + this.name
               + ", type=" + this.type + '}';
    }

    @Override
    public int hashCode() {
        int result = this.url != null ? this.url.hashCode() : 0;
        result = 31 * result + (this.action != null ? this.action.hashCode() : 0);
        result = 31 * result + (this.name != null ? this.name.hashCode() : 0);
        result = 31 * result + (this.type != null ? this.type.hashCode() : 0);
        return result;
    }

	@Override
	public long getLength() {
		return Long.MAX_VALUE;
	}

	@Override
	public String[] getLocations() {
		String[] urls = new String[1];
		urls[0] = this.name;
		return urls;
	}

	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}


}
