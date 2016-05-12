package org.hammer.isabella.query;

import java.util.SortedMap;

/**
 * Label Value Node - (from WHERE-clause)
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class LabelValueNode extends ValueNode {

	/**
	 * Build a label node
	 * @param instance
	 * @param name
	 * @param line
	 * @param column
	 */
	public LabelValueNode(String instance, String name, int line, int column) {
		super(name, 1.0f, 0.5f, 0.0f, line, column);
		this.instance = instance;
	}
	
	/**
	 * Instance
	 */
	public String instance = "";

	/**
	 * Get my instance
	 * @return
	 */
	public String getInstance() {
		return instance;
	}

	/**
	 * Set my instance
	 * @param instance
	 */
	public void setInstance(String instance) {
		this.instance = instance;
	}

	@Override
	public void eval(SortedMap<Integer, IsabellaError> errorList, int line, int column) {
		if(super.getName() == null || super.getName().trim().length()<=0) {
			errorList.put((line * 1000) + 1, new IsabellaError(line, column, "Label2 not defined; \"label2\" : \"xxxxxx\""));
		}
		if(instance == null || instance.trim().length()<=0) {
			errorList.put((line * 1000) + 1, new IsabellaError(line, column, "Instance2 not defined; \"instance2\" : \"xxxxxx\""));
		}	

	}

}
