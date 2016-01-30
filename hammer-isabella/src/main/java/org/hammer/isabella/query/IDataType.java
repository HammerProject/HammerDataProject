package org.hammer.isabella.query;

import java.util.SortedMap;


/**
 * Model for all data type
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public interface IDataType {
	
	
	/**
	 * Eval data and compile error list
	 * @param errorList
	 */
	public void eval(SortedMap<Integer, IsabellaError> errorList, int line, int column);

}
