package org.hammer.isabella.query;

import java.util.SortedMap;


/**
 * Isabella Error Parser
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class IsabellaError implements IDataType {
	
	/**
	 * 
	 * @param line
	 * @param column
	 * @param message
	 */
	public IsabellaError(int line, int column, String message) {
		this.line = line;
		this.column = column;
		this.message = message;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "!!! ERROR [line=" + line + ", column=" + column + "] !!! ---> " + message + "";
	}

	/**
	 * Row
	 */
	private int line = 0;
	
	/**
	 * Colomn
	 */
	private int column = 0;
	
	/**
	 * Message
	 */
	private String message  = "";

	/**
	 * @return the line
	 */
	public int getLine() {
		return line;
	}

	/**
	 * @param line the line to set
	 */
	public void setLine(int line) {
		this.line = line;
	}

	/**
	 * @return the column
	 */
	public int getColumn() {
		return column;
	}

	/**
	 * @param column the column to set
	 */
	public void setColumn(int column) {
		this.column = column;
	}

	/**
	 * @return the message
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * @param message the message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public void eval(SortedMap<Integer, IsabellaError> errorList, int line, int column) {
	}
	

	
}
