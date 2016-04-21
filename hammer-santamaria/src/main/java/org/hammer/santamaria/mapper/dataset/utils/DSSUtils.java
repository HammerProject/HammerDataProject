package org.hammer.santamaria.mapper.dataset.utils;

import java.util.ArrayList;
import java.util.StringTokenizer;


/**
 * DSS Utils tool kit
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class DSSUtils {

	/**
	 * Return DSS name by meta
	 * 
	 * @param meta
	 * @return
	 */
	public static DSS CheckDSSByMeta(ArrayList<String> meta) {
		if(meta == null) {
			return DSS.UNDEFINED;
		}
		if((meta.size() == 2)&&(meta.contains("dataset_id"))&&(meta.contains("data"))) {
			return DSS.ALBANO_LAZIALE;
		}
		
		
		return DSS.UNDEFINED;
	}
	
	/**
	 * Extract keywords from text
	 * 
	 * @param text
	 * @return
	 */
	public static ArrayList<String> GetKeyWordsFromText(String text){
		ArrayList<String> myKeys = new ArrayList<String>();
		if(text == null) {
			return myKeys;
		}
		StringTokenizer st = new StringTokenizer(text, " ");
		
		while (st.hasMoreElements()) {
			String tW = st.nextToken();
			StringTokenizer st1 = new StringTokenizer(tW, "-.,!_");
			while (st1.hasMoreElements()) {
				String word = st1.nextToken();
				if(word.length() >= 3) {
					myKeys.add(word.toLowerCase());
				}
			}
		}
		
		return myKeys;
	}
	
	
	

}
