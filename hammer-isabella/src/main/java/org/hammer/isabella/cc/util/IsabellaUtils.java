package org.hammer.isabella.cc.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Calendar;
import java.util.Date;
import java.util.SortedMap;
import java.util.TreeMap;

import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.cc.Token;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.QueryGraph;


/**
 * Util
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class IsabellaUtils {

	
	
	/**
	 * File to string funcions
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static String readFile(String fileName) throws IOException {
		BufferedReader br = null;
		StringBuilder sb = null;
		try {
			br = new BufferedReader(new FileReader(fileName));
			sb = new StringBuilder();
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}
			return sb.toString();
		} finally {
			sb = null;
			if (br != null)
				br.close();
		}
	}

	/**
	 * Return today
	 * 
	 * @return
	 */
	public static final Date now() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}

	/**
	 * Process error message
	 * @param parser
	 * @param errors
	 * @param ex
	 * @param recoveryPoint
	 */
	public static final void Recover(Isabella parser, SortedMap<Integer, IsabellaError> errors, ParseException ex, int recoveryPoint) {
		if (errors == null)
			errors = new TreeMap<Integer, IsabellaError>();
		Token t = parser.token;
		Token n = (t.next != null) ? t.next : t;
		IsabellaError err = new IsabellaError(n.beginLine, n.beginColumn, ex.getMessage());
		errors.put((t.beginLine * 1000), err);
		while (t.kind != Isabella.EOF && t.kind != recoveryPoint) {
			t = parser.getNextToken();
		}
	}

	/**
	 * Parse Message and get obj
	 * @param expression
	 * @return
	 * @throws Exception
	 */
	public static QueryGraph ParseMessage(String expression) throws Exception {
		Isabella parser = new Isabella(new StringReader(expression));
		QueryGraph doc = parser.queryGraph();
		
		
		for (IsabellaError ex : parser.getErrors().values()) {
			System.out.println(ex.toString());
		}
		return doc;
	}


}
