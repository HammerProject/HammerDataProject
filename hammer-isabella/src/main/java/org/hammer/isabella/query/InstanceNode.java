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
	 * Build the instance node
	 * @param name
	 * @param line
	 * @param column
	 */
	public InstanceNode(String name, int line, int column) {
		super(name, 1.0f, 0.6f, line, column);
	}

	
}
