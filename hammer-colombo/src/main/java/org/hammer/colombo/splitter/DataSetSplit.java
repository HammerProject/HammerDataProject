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
 * 
 * Data Set Split
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Colombo
 *
 */
public class DataSetSplit extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit {
	
	/**
	 * Build the data setsplit
	 */
    public DataSetSplit() {
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
     * Data source
     */
    private String datasource = "";
    
	/**
	 * My name
	 */
	private String name = "";

    /**
     * Format type (XML, JSON, ...)
     */
    private String type = "";
    
    /**
     * Original Action from Datasource
     */
    private String action = "";
    
    public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getDataset() {
		return dataset;
	}

	public void setDataset(String dataset) {
		this.dataset = dataset;
	}

	/**
     * Original Dataset name from Datasource
     */
    private String dataset = "";
    
    /**
     * Url
     */
    private String url = "";
    
    /**
     * Type of dataset (Socrata, CKAN, ...)
     */
    private String dataSetType ="";
    
    
    
    /**
     * Get my url
     * @return
     */
    public String getUrl() {
		return url;
	}

    /**
     * Set my url
     * @param url
     */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * Get my name
	 * @return
	 */
	public String getName() {
		return name;
	}

	/**
	 * Set my name
	 * @param name
	 */
	public void setName(String name) {
		this.name = name;
	}

    /**
     * Write to buffer
     */
    public void write(final DataOutput out) throws IOException {
        BSONObject spec = BasicDBObjectBuilder.start()
                                              .add("name", getName())
                                              .add("url", getUrl() != null ? getUrl() : null)
                                              .add("type", getType() != null ? getType() : "")
                                              .add("dataset_type", getDataSetType() != null ? getDataSetType() : "")
                                              .add("datasource", getDatasource() != null ? getDatasource() : "")
                                              .add("dataset", getDataset() != null ? getDataset() : "")
                                              .add("action", getAction() != null ? getAction() : "")
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
        setName(spec.get("name").toString());
        setUrl(spec.get("url").toString());
        setType(spec.get("type").toString());
        setDataSetType(spec.get("dataset_type").toString());
        setDatasource(spec.get("datasource").toString());
        setDataset(spec.get("dataset").toString());
        setAction(spec.get("action").toString());

    }


    @Override
    public String toString() {
        return "DataSetSplit{URL=" + this.url.toString()
        	   + ", datasource=" + this.datasource
        	   + ", type=" + this.type
        	   + ", dataset=" + this.dataset
        	   + ", action=" + this.action
        	   + ", dataset_type=" + this.dataSetType
               + ", name=" + this.name + '}';
    }

    @Override
    public int hashCode() {
        int result = this.url != null ? this.url.hashCode() : 0;
        result = 31 * result + (this.name != null ? this.name.hashCode() : 0);
        result = 31 * result + (this.type != null ? this.type.hashCode() : 0);
        result = 31 * result + (this.datasource != null ? this.datasource.hashCode() : 0);
        result = 31 * result + (this.dataSetType != null ? this.dataSetType.hashCode() : 0);
        result = 31 * result + (this.dataset != null ? this.dataset.hashCode() : 0);
        result = 31 * result + (this.action != null ? this.action.hashCode() : 0);
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
	 * Get my type (XML, JSON, ..)
	 * @return
	 */
	public String getType() {
		return type;
	}

	/**
	 * Set my type (XML, JSON, ..)
	 * @param type
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * Return dataset type (Socrata, CKAN ,...)
	 * @return
	 */
	public String getDataSetType() {
		return dataSetType;
	}

	/**
	 * Set dataset type (Socrata, CKAN ,...)
	 * @param dataSetType
	 */
	public void setDataSetType(String dataSetType) {
		this.dataSetType = dataSetType;
	}

	public String getDatasource() {
		return datasource;
	}

	public void setDatasource(String datasource) {
		this.datasource = datasource;
	}


}
