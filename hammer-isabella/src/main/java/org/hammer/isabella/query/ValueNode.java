package org.hammer.isabella.query;


/**
 * Text Value Node - (from WHERE-clause)
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public abstract  class ValueNode extends Node {

	/**
	 * Build a Value Node
	 * 
	 * @param name
	 * @param riScore
	 * @param reScore
	 * @param line
	 * @param column
	 */
	public ValueNode(String name, float riScore, float reScore, int line, int column) {
		super(name, 0.8f, riScore, reScore, line, column);
	}
	
	/**
	 * Build a Value Node
	 * 
	 * @param name
	 * @param iScore
	 * @param riScore
	 * @param reScore
	 * @param line
	 * @param column
	 */
	public ValueNode(String name, float iScore, float riScore, float reScore, int line, int column) {
		super(name, iScore, riScore, reScore, line, column);
	}


}
