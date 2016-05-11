package org.hammer.isabella.query;

/**
 * A keyword
 * 
 * @author mauropelucchi
 *
 */
public class Keyword {

	/**
	 * My reScore
	 */
	private float reScore = 0.0f;
	
	public float getReScore() {
		return reScore;
	}

	/**
	 * Creare a keyword
	 * 
	 * @param reScore
	 * @param keyword
	 */
	public Keyword(String keyword, long totalResources, long keywordResources) {
		super();
		if(keywordResources == 0) {
			this.reScore = 0;
		} else {
			this.reScore = log(((keywordResources / totalResources) + 1), 2);
		}
		this.keyword = keyword;
	}
	
	/**
	 * Log 
	 * 
	 * @param x
	 * @param base
	 * @return
	 */
	static float log(long x, int base)
	{
	    return (float) (Math.log(x) / Math.log(base));
	}
	
	/**
	 * Set my re-score
	 * @param reScore
	 */
	public void setReScore(float reScore) {
		this.reScore = reScore;
	}

	/**
	 * My Keyword
	 */
	private String keyword = "";

	public String getKeyword() {
		return keyword;
	}

	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}
}
