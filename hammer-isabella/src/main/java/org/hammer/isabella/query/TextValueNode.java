package org.hammer.isabella.query;


/**
 * Value Node - (from WHERE-clause)
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class TextValueNode extends ValueNode {

	/**
	 * Build a Text Value Node
	 * @param name
	 * @param line
	 * @param column
	 */
	public TextValueNode(String name, int line, int column) {
		super(name, 0.5f, 0.0f, line, column);
	}


}
