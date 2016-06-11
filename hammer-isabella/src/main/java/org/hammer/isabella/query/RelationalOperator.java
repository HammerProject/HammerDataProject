package org.hammer.isabella.query;

import java.io.Serializable;

/**
 * List of Relational Operator
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public enum RelationalOperator implements Serializable {
	eq, // =
	ge, // >=
	gt, // >
	le, // <=
	lt, // <
	ne // <>
}
