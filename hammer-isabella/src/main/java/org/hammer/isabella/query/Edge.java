package org.hammer.isabella.query;

import java.util.SortedMap;

/**
 * Edge
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class Edge extends Node {
	
	/**
	 * Instance
	 */
	public String instance = "";

	
	/**
	 * Operator (=, <=, ...)
	 */
	private String operator = "=";
	
	/**
	 * Logical operator
	 */
	private String condition = "OR";


	@Override
	public void eval(SortedMap<Integer, IsabellaError> errorList, int line, int column) {
		if (super.getName() == null || super.getName().trim().length() <= 0) {
			errorList.put((line * 1000) + 1,
					new IsabellaError(line, column, "Label1 non defined; \"label1\" : \"xxxxx\""));
		}
		if (instance == null || instance.trim().length() <= 0) {
			errorList.put((line * 1000) + 1,
					new IsabellaError(line, column, "Instance not defined; \"instance\" : \"xxxxxx\""));
		}
		if (operator == null || operator.trim().length() <= 0) {
			errorList.put((line * 1000) + 1,
					new IsabellaError(line, column, "Operator not defined; \"operator\" : \"gt|ge|lt|le|eq|ne\""));
		}
		if (condition == null || condition.trim().length() <= 0) {
			errorList.put((line * 1000) + 1,
					new IsabellaError(line, column, "Logical Operator not defined; \"logicalOperator\" : \"or|and\""));
		}

	}

	/**
	 * Build a Edge
	 * 
	 * @param name my name
	 * @param line
	 * @param column
	 */
	public Edge(String name, int line, int column) {
		super(name, 1.0f, 0.8f, 0.0f, line, column);
	}

	/**
	 * Get my operator
	 * @return
	 */
	public String getOperator() {
		return operator;
	}

	/**
	 * Set my operator
	 * @param operator
	 */
	public void setOperator(String operator) {
		this.operator = operator;
	}

	@Override
	public int hashCode() {
		int result = this.getName() != null ? this.getName().hashCode() : 0;
		result = 31 * result + (this.operator != null ? this.operator.hashCode() : 0);
		result = 31 * result + (this.condition != null ? this.condition.hashCode() : 0);
		result = 31 * result + (this.instance != null ? this.instance.hashCode() : 0);
		for (Node node : getChild()) {
			result = 31 * result + node.hashCode();
		}
		return result;
	}

	@Override
	public void test(int level) {
		int l = level+1;
		this.setSelected(false);
		for(int i = 0; i < level; i++) {
			System.out.print("-");
		}
		System.out.println(("-> " + this.getCondition()).trim() + " " + this.instance + "." + this.getName() + " " + this.getOperator() + "(" + this.getiScore() + ", " + this.getriScore() + ", " + this.getreScore() + ")");
		for (Node node : getChild()) {
			node.test(l);
		}
	}

	/**
	 * Get condition
	 * @return
	 */
	public String getCondition() {
		return condition;
	}

	/**
	 * Set condition
	 * @param condition
	 */
	public void setCondition(String condition) {
		this.condition = condition;
	}

	/**
	 * Get my instance
	 * @return
	 */
	public String getInstance() {
		return instance;
	}

	/**
	 * Set my instance
	 * 
	 * @param instance
	 */
	public void setInstance(String instance) {
		this.instance = instance;
	}

}
