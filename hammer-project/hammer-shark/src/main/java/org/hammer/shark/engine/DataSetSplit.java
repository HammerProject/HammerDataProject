package org.hammer.shark.engine;

import java.io.Serializable;

/**
 * 
 * 
 * Data Set Split
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Shark
 *
 */
public class DataSetSplit implements Serializable {
	
	/**
	 * Build the data setsplit
	 */
    public DataSetSplit() {
    }
    
    private long size = 0;

    public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

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
