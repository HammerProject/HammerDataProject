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
	 * 
	 */
	private static final long serialVersionUID = 1636442874685152781L;

	/**
	 * Build a Number Value Node
	 * @param name
	 * @param line
	 * @param column
	 */
	public NumberValueNode(String name, int line, int column) {
		super(name, 0.2f, 0.0f, line, column);
	}

}
