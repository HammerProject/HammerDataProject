package org.hammer.isabella.query;

/**
 * Question node (from SELECT-clause)
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class QuestionNode extends Node {

	/**
	 * Build a Question Node
	 * @param name
	 * @param iScore
	 * @param rScore
	 * @param line
	 * @param column
	 */
	public QuestionNode(String name, float iScore, float riScore, float reScore, int line, int column) {
		super(name, iScore, riScore, reScore, line, column);
	}

}
