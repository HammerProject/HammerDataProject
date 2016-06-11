package org.hammer.colombo.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * Recursive String
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class RecursiveString {
	
	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {
		ArrayList<String> VariableA = new ArrayList<String>(Arrays.asList("red", "green"));
		ArrayList<String> VariableB = new ArrayList<String>(Arrays.asList("A", "B", "C"));
		ArrayList<String> VariableC = new ArrayList<String>(Arrays.asList("1", "2", "3", "4", "5"));

		Map<String, ArrayList<String>> AofA = new HashMap<String, ArrayList<String>>();
		AofA.put("k1", VariableA);
		AofA.put("k2", VariableB);
		AofA.put("k3", VariableC);

		System.out.println("Array of Arrays: ToString(): " + AofA.toString());

		ArrayList<String[]> optionsList = new ArrayList<String[]>();

		// recursive call
		ArrayList<ArrayList<String[]>> testCases = new ArrayList<ArrayList<String[]>>();
		Recurse(optionsList, AofA, 0, testCases);

		for (int i = 0; i < testCases.size(); i++) {
			System.out.println("Test Case " + (i + 1) + ": ");
			for(String[] k : testCases.get(i)) {
				System.out.println(k[0] + "-" + k[1] + ",");
			}
			System.out.println("\n");
		}

	}

	public static void Recurse(ArrayList<String[]> newOptionsList, Map<String, ArrayList<String>> newAofA,
			int placeHolder, ArrayList<ArrayList<String[]>> testCases) {
		
		// check to see if we are at the end of all TestAspects
		if (placeHolder < newAofA.size()) {

			// remove the first item in the ArrayOfArrays
			String k = (String) newAofA.keySet().toArray()[placeHolder];
			ArrayList<String> currentAspectsOptions = newAofA.get(k);
			// iterate through the popped off options

			for (int i = 0; i < currentAspectsOptions.size(); i++) {
				ArrayList<String[]> newOptions = new ArrayList<String[]>();
				// add all the passed in options to the new object to pass on
				for (int j = 0; j < newOptionsList.size(); j++) {
					newOptions.add(newOptionsList.get(j));
				}

				String[] t = new String[2];
				t[0] = k;
				t[1] = currentAspectsOptions.get(i);
				newOptions.add(t);
				int newPlaceHolder = placeHolder + 1;
				Recurse(newOptions, newAofA, newPlaceHolder, testCases);
			}
		} else { // no more arrays to pop off
			ArrayList<String[]> newTestCase = new ArrayList<String[]>();
			for (int i = 0; i < newOptionsList.size(); i++) {
				String[] t = new String[2];
				t[0] = newOptionsList.get(i)[0];
				t[1] = newOptionsList.get(i)[1];

				newTestCase.add(t);
			}
			System.out.println("\t### Adding: " + newTestCase.toString());
			
			testCases.add(newTestCase);
		}
		System.out.println("\t### Total:  " + testCases.size());
	}
}