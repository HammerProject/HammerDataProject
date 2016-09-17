package org.hammer.isabella.fuzzy;

import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * 2Gram
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 */
public class NGram {



	public static ArrayList<String> ngram2(String text) {
		ArrayList<String> terms = new ArrayList<String>();
		ArrayList<String> results = new ArrayList<String>();
		StringTokenizer st = new StringTokenizer(text, " ");

		while (st.hasMoreElements()) {
			String tW = st.nextToken();
			StringTokenizer st1 = new StringTokenizer(tW, "-.,!_");
			while (st1.hasMoreElements()) {
				String word = st1.nextToken();
				if (word.length() >= 3) {
					word = word.replaceAll("[^a-zA-Z0-9]", "");
					if(!terms.contains(word.toLowerCase())) {
						terms.add(word.toLowerCase());
					}
				}
			}
		}
		if(terms.size() == 1) {
			results.add(terms.get(0));
		}
		for(int i = 0; i < (terms.size() - 1); i++) {
			String temp = terms.get(i);
			temp += " ";
			temp += terms.get(i + 1);
			results.add(temp);
			System.out.println(temp);
		}
		return results;
	}
	
	public static void main( String[] args )
    {
		NGram.ngram2("8 minutes till ios10");
    
    }
}
