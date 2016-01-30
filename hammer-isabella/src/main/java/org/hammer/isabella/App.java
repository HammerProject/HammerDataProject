package org.hammer.isabella;

import java.io.StringReader;

import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.util.IsabellaUtils;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.QueryGraph;

/**
 * App
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class App {
	/**
	 * Test expression
	 */
	static String expression[] = {
			"{\"select\":[\"abitanti\",\"superficie\"],\"from\":[\"comuni\",\"superfici edificatibili\"],\"where\":[{\"label\":\"localita\",\"operator\":\"=\",\"value\":\"carvico\",\"condition\":\"OR\"},{\"label\":\"localita\",\"operator\":\"=\",\"value\":\"calusco d'adda\",\"condition\":\"AND\"}]}",
			"{\"select\":[\"abitanti\",\"superficie\"],\"from\":[\"comuni\",\"superfici edificatibili\"],\"where\":[]}",
			"{\"select\":[],\"from\":[\"comuni\",\"superfici edificatibili\"],\"where\":[]}",
			"{\"select\":[\"*\"],\"from\":[\"comuni\",\"superfici edificatibili\"],\"where\":[]}"

	};
	
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        try {
			App.testFile();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
        /*
        for (String exp : expression) {
			try {
				System.out.println(exp);
				App.test(exp);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}*/
    }
    
    
    /**
	 * UNIT TEST METHOD
	 * 
	 * @param expression, a json string
	 * @throws Exception
	 */
	public static void test(String expression) throws Exception {
		Isabella parser = new Isabella(new StringReader(expression));
		QueryGraph q = parser.queryGraph();
		q.test();
		for(IsabellaError err : parser.getErrors().values()) {
			System.out.println(err.toString());
		}
		
		//q.getQueryCondition();
		//q.labelSelection();
		System.out.println("ok");

	}
	
	/**
	 * UNIT TEST METHOD
	 * 
	 * @param expression, a json string
	 * @throws Exception
	 */
	public static void testFile() throws Exception {
		Isabella parser = new Isabella(new StringReader(IsabellaUtils.readFile("query.json")));
		QueryGraph q = parser.queryGraph();
		
		for(IsabellaError err : parser.getErrors().values()) {
			System.out.println(err.toString());
		}
		q.test();
        //q.getQueryCondition();
        q.labelSelection();
		System.out.println("ok");

	}
}
