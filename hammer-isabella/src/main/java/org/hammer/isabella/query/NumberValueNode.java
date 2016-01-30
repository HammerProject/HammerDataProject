package org.hammer.isabella.query;

/**
 * Number Value Node - (from WHERE-clause)
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class NumberValueNode extends ValueNode {

	/**
	 * Build a Number Value Node
	 * @param name
	 * @param line
	 * @param column
	 */
	public NumberValueNode(String name, int line, int column) {
		super(name, 0.0f, line, column);
	}

}
