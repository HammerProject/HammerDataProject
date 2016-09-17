package org.hammer.colombo.utils;

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * Recursive String
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class RecursiveString {
	
	public static final Log LOG = LogFactory.getLog(RecursiveString.class);
	

	public static void Recurse(ArrayList<Term[]> newOptionsList, Map<String, ArrayList<Term>> newAofA,
			int placeHolder, ArrayList<ArrayList<Term[]>> testCases) {
		
		// check to see if we are at the end of all TestAspects
		if (placeHolder < newAofA.size()) {

			// remove the first item in the ArrayOfArrays
			Term k = (Term) newAofA.keySet().toArray()[placeHolder];
			ArrayList<Term> currentAspectsOptions = newAofA.get(k);
			// iterate through the popped off options

			for (int i = 0; i < currentAspectsOptions.size(); i++) {
				ArrayList<Term[]> newOptions = new ArrayList<Term[]>();
				// add all the passed in options to the new object to pass on
				for (int j = 0; j < newOptionsList.size(); j++) {
					newOptions.add(newOptionsList.get(j));
				}

				Term[] t = new Term[2];
				t[0] = k;
				t[1] = currentAspectsOptions.get(i);
				newOptions.add(t);
				int newPlaceHolder = placeHolder + 1;
				Recurse(newOptions, newAofA, newPlaceHolder, testCases);
			}
		} else { // no more arrays to pop off
			ArrayList<Term[]> newTestCase = new ArrayList<Term[]>();
			for (int i = 0; i < newOptionsList.size(); i++) {
				Term[] t = new Term[2];
				t[0] = newOptionsList.get(i)[0];
				t[1] = newOptionsList.get(i)[1];

				newTestCase.add(t);
			}
			LOG.debug("\t### Adding: " + newTestCase.toString());
			
			testCases.add(newTestCase);
		}
		LOG.debug("\t### Total:  " + testCases.size());
	}
	
	
	public static void RecurseWithString(ArrayList<String[]> newOptionsList, Map<String, ArrayList<String>> newAofA,
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
				RecurseWithString(newOptions, newAofA, newPlaceHolder, testCases);
			}
		} else { // no more arrays to pop off
			ArrayList<String[]> newTestCase = new ArrayList<String[]>();
			for (int i = 0; i < newOptionsList.size(); i++) {
				String[] t = new String[2];
				t[0] = newOptionsList.get(i)[0];
				t[1] = newOptionsList.get(i)[1];

				newTestCase.add(t);
			}
			LOG.debug("\t### Adding: " + newTestCase.toString());
			
			testCases.add(newTestCase);
		}
		LOG.debug("\t### Total:  " + testCases.size());
	}
}