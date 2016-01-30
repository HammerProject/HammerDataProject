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
	 * @param rScore
	 * @param line
	 * @param column
	 */
	public ValueNode(String name, float rScore, int line, int column) {
		super(name, 1.0f, rScore, line, column);
	}


}
