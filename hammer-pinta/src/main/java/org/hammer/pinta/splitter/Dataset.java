package org.hammer.pinta.splitter;

import java.util.ArrayList;
import java.util.Date;

import com.mongodb.BasicDBObject;

public class Dataset extends BasicDBObject {

    /**
	 * 
	 */
	private static final long serialVersionUID = 759293235189379968L;
	
	public static final String ID = "id";
	public static final String TAGS = "tags";
	public static final String META = "meta";
	public static final String COLLECTION_NAME = "data";
	public static final String OTHER_TAGS = "other_tags";
	public static final String DATASETTYPE = "datasettype";
	public static final String UPDATE = "update";
    


	public String getId() {
		return this.getString(ID);
	}


	public void setId(String id) {
		this.put(ID, id);
	}
	
	
	public Date getUpdate() {
		return this.getDate(UPDATE);
	}


	public void setUpdate(Date id) {
		this.put(UPDATE, id);
	}
	
	public String getDatasetType() {
		return this.getString(DATASETTYPE);
	}


	public void setDatasetType(String id) {
		this.put(DATASETTYPE, id);
	}


	@SuppressWarnings("unchecked")
	public ArrayList<String> getTags() {
		return (ArrayList<String>) this.get(TAGS);
	}


	public void setTags(ArrayList<String> tags) {
		this.put(TAGS, tags);
	}
	
	
	
	@SuppressWarnings("unchecked")
	public ArrayList<String> getMeta() {
		return (ArrayList<String>) this.get(META);
	}


	public void setMeta(ArrayList<String> tags) {
		this.put(META, tags);
	}
	
	@SuppressWarnings("unchecked")
	public ArrayList<String> getOtherTags() {
		return (ArrayList<String>) this.get(OTHER_TAGS);
	}


	public void setOtherTags(ArrayList<String> other_tags) {
		this.put(OTHER_TAGS, other_tags);
	}
}
