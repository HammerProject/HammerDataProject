package org.hammer.isabella.query;

import java.util.SortedMap;

/**
 * Question Edge - connect a question with a instance
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class QuestionEdge extends Node {
	
	/**
	 * Instance
	 */
	public String instance = "";


	@Override
	public void eval(SortedMap<Integer, IsabellaError> errorList, int line, int column) {
		if (super.getName() == null || super.getName().trim().length() <= 0) {
			errorList.put((line * 1000) + 1,
					new IsabellaError(line, column, "Label non defined; \"label1\" : \"xxxxx\""));
		}
		if (instance == null || instance.trim().length() <= 0) {
			errorList.put((line * 1000) + 1,
					new IsabellaError(line, column, "Instance not defined; \"instance\" : \"xxxxxx\""));
		}

	}

	/**
	 * Build a Question Edge
	 * 
	 * @param name my name
	 * @param line
	 * @param column
	 */
	public QuestionEdge(String name, int line, int column) {
		super(name, 0.8f, 0.5f, 0.0f, line, column);
	}

	@Override
	public int hashCode() {
		int result = this.getName() != null ? this.getName().hashCode() : 0;
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
		System.out.println("-> " + this.instance + "." + this.getName() + " (" + this.getiScore() + ", " + this.getriScore() + ", " + this.getreScore() + ")");
		for (Node node : getChild()) {
			node.test(l);
		}
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
