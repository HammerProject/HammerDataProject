package org.hammer.isabella;

import java.io.StringReader;

import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.util.IsabellaUtils;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Keyword;
import org.hammer.isabella.query.QueryGraph;

/**
 * App
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class App {
	
	
    public static void main( String[] args )
    {
    	Keyword k = new Keyword("test", 77017, 22);
    	k.toString();
        System.out.println( "!!!! Test Isabella Parser !!!!" );
        
        try {
            if((args == null) || (args.length < 1)) {
            	throw new Exception("Syntax error: use <path to query file>");
            }
			App.testFile(args[0]);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
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
	public static void testFile(String filePath) throws Exception {
		Isabella parser = new Isabella(new StringReader(IsabellaUtils.readFile(filePath)));
		QueryGraph q = parser.queryGraph();
		
		for(IsabellaError err : parser.getErrors().values()) {
			System.out.println(err.toString());
		}
		q.test();
        //q.getQueryCondition();
        q.labelSelection();
		System.out.println("ok !!!!");
		q.calculateMyLabels();

	}
}
