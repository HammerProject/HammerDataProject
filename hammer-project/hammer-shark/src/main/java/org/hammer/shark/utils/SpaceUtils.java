package org.hammer.shark.utils;

import java.util.List;

import org.hammer.isabella.query.Term;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class SpaceUtils {	

	/**
	 * Log
	 */
	private static final Log LOG = LogFactory.getLog(SpaceUtils.class);

	
	/**
	 * cos(angle(v(s), v(q)).
	 * 
	 * Suppose that the main query "q" has [1,1,1,1,1,1,...]
	 * Calc cos between q and q' : it is the similarity!
	 */
	public static double cos(List<Term[]> qtest) {
		
		String x_ = "[";
		String y_ = "[";
		double xy = 0.0d;
		// calc q vector
		double[]  x = new double[qtest.size()];
		double[]  y = new double[qtest.size()];
		double xbar = 0.0d;
		double ybar = 0.0d;
		for(int i = 0; i<x.length;i++) {
			x[i] = 1.0d;
			y[i] = qtest.get(i)[1].getWeigth();
			
			x_ += "," + x[i];
			y_ += "," + y[i];
			
			xy += x[i] * y[i];
			xbar += (Math.pow(x[i], 2));
			ybar += (Math.pow(y[i], 2));
		}
		xbar = Math.sqrt(xbar);
		ybar = Math.sqrt(ybar);
		
		double cosTheta = (xy) / (xbar * ybar);
		
		LOG.debug("--------------------------------------");
		LOG.debug(x_);
		LOG.debug(y_);
		LOG.debug("cos(th) - " + cosTheta);
		LOG.debug("--------------------------------------");

		
		return cosTheta;
	}
}
