package org.hammer.shark.query;

import java.io.Serializable;

/**
 * 
 * Query Split
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Shark
 *
 */
public class QuerySplit implements Serializable {
	
	/**
	 * Build the query split
	 */
    public QuerySplit() {
    }
    
    /**
     * Query
     */
    private String queryString = "";
    
    /**
     * The keywords set
     */
    private String keywords = "";
    
    public String getKeywords() {
		return keywords;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	/**
     * Get my query string
     * @return
     */
    public String getQueryString() {
		return queryString;
	}

	/**
	 * Set my query string
	 * @param queryString
	 */
	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}


}
