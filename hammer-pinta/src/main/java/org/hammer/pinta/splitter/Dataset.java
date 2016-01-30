package org.hammer.pinta.splitter;

import java.util.ArrayList;


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
    


	public String getId() {
		return this.getString(ID);
	}


	public void setId(String id) {
		this.put(ID, id);
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
}
