package org.hammer.core.model;


/**
 * 
 * Data Source Model
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Core
 *
 */
public class DataSource {


	/**
	 * Name of data source
	 */
	private String name;
	
	/**
	 * Url (point to dataset catalog)
	 */
	private String url;

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
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

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	private String type = "";
	
	
	private String action = "";
	
}
