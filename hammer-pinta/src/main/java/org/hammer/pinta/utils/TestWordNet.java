package org.hammer.pinta.utils;

import java.io.IOException;

public class TestWordNet {

	
	public static void main(String [] args) throws IOException {
		
		String wnhome = "/Users/mauropelucchi/Desktop/My_Home/Tools/WordNet-3.0";
		String term = "healthcare";
		for(String t : WordNetUtils.MySynset(wnhome, term).keySet()) {
			System.out.println(t);
		}
		
	} 
}
