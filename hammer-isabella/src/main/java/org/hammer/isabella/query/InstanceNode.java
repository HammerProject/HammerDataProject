package org.hammer.isabella.query;

/**
 * Instance node (from FROM clause)
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class InstanceNode extends Node {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4894726710457152066L;

	/**
	 * Build the instance node
	 * @param name
	 * @param line
	 * @param column
	 */
	public InstanceNode(String name, int line, int column) {
		super(name, 1.0f, 1.0f, 0.0f, line, column);
	}

	
}
