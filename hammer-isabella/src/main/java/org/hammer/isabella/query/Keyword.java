package org.hammer.isabella.query;

import java.io.Serializable;

/**
 * A keyword
 * 
 * @author mauropelucchi
 *
 */
public class Keyword implements Cloneable, Serializable {
	
    /**
	 * 
	 */
	private static final long serialVersionUID = -3767440922897597489L;

	@Override public Keyword clone() {
        try {
            final Keyword result = (Keyword) super.clone();
            result.keyword = this.keyword;
            result.reScore = this.reScore;
            result.similarity = 0.0d;
            return result;
        } catch (final CloneNotSupportedException ex) {
            throw new AssertionError();
        }
    }

	@Override
	public String toString() {
		return "Keyword [reScore=" + reScore + ", keyword=" + keyword + "]";
	}

	/**
	 * My reScore
	 */
	private double reScore = 0.0f;
	
	public double getReScore() {
		return reScore;
	}

	/**
	 * Creare a keyword
	 * 
	 * @param reScore
	 * @param keyword
	 */
	public Keyword(String keyword,long totalResources, long keywordResources) {
		super();
		//System.out.println("-------------------------------------------");
		//System.out.println("---> " + totalResources);
		//System.out.println("---> " + keywordResources);
		if(keywordResources == 0) {
			this.reScore = 0;
		} else {
			//System.out.println(( Math.log(1.000285614f) / Math.log(2f)));
			//System.out.println(((double)keywordResources / (double)totalResources) + 1.0f);
			//System.out.println(log2((((double)keywordResources / (double)totalResources) + 1.0f)));
			this.reScore = (1 - log2((((double)keywordResources / (double)totalResources) + 1.0f)));
		}
		this.keyword = keyword;
		//System.out.println("---> " + toString());
	}
	
	/**
	 * Log 
	 * 
	 * @param x
	 * @param base
	 * @return
	 */
	static double log2(double x)
	{
	    return (Math.log(x) / Math.log(2f));
	}
	
	/**
	 * Set my re-score
	 * @param reScore
	 */
	public void setReScore(double reScore) {
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
	
	public double getSimilarity() {
		return similarity;
	}

	public void setSimilarity(double similarity) {
		this.similarity = similarity;
	}

	/**
	 * Similarity
	 */
	private double similarity = 0.0d;
	
	
}
