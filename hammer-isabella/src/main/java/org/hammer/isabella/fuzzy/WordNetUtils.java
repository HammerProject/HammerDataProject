package org.hammer.isabella.fuzzy;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.mit.jwi.Dictionary;
import edu.mit.jwi.IDictionary;
import edu.mit.jwi.item.IIndexWord;
import edu.mit.jwi.item.IWord;
import edu.mit.jwi.item.IWordID;
import edu.mit.jwi.item.POS;


/**
 * Proxy to use WordNet to calc synset of a term
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Pinta
 *
 */
public class WordNetUtils {

	public static final Log LOG = LogFactory.getLog(WordNetUtils.class);

	/**
	 * Calc Synset
	 * 
	 * @param wnhome the wnhome path
	 * @param term a term
	 * @return
	 * @throws IOException
	 */
	public static Map<String, Double> GetSynset(String wnhome, String term) throws IOException {
		Map<String, Double> mylist = new HashMap<String, Double>();
		String path = wnhome + File.separator + "dict"; URL url = new URL("file", null, path);
		IDictionary dict = null;
		try {
			dict = new Dictionary(url);
			dict.open();
			IIndexWord idxWord = dict.getIndexWord(term, POS.NOUN);
			if (idxWord == null) {
				return mylist;
			}
			for(IWordID wordID: idxWord.getWordIDs()) {
				IWord word = dict.getWord(wordID);

					for(IWord w : word.getSynset().getWords()) {
						mylist.put(w.getLemma().replaceAll("_",""),1.0d);
						
					}
			}
			
		} catch (Exception e) {
			LOG.error(e);
			e.printStackTrace();
			/* do nothing */
		} finally {
			try { if (dict != null) { dict.close(); } } catch (Exception ex) { /* do nothing */ }
		}
		return mylist;
	}
	
	
	/**
	 * Return a synset for Pinta
	 * 
	 * @param wnhome the word net path
	 * @param term a term
	 * @return a map of term
	 */
	public static Map<String, Double> MySynset(String wnhome, String term) {
		Map<String, Double> myList = new HashMap<String, Double>();
		try {
			term = term.replaceAll("[^a-zA-Z]", "").toLowerCase();
			myList = WordNetUtils.GetSynset(wnhome, term);

			for(int i = 3; i < term.length(); i++) {
				String termS = term.substring(0, i);
				String termE = term.substring(i, term.length());
				String termC = termS + "_" + termE;
				
				Map<String, Double> myListS = new HashMap<String, Double>();
				Map<String, Double> myListE = new HashMap<String, Double>();
				Map<String, Double> myListC = new HashMap<String, Double>();

				if (termS.length() >= 3) { myListS = WordNetUtils.GetSynset(wnhome, termS); }
				if (termE.length() >= 3) { myListE = WordNetUtils.GetSynset(wnhome, termE); }
				if (termC.length() >= 3) { myListC = WordNetUtils.GetSynset(wnhome, termC); }
				
				if (myListS.size() > 0 && myListE.size() > 0) {
					double l = myListS.size() + myListE.size();
					for(String s : myListS.keySet()) {
						myList.put(s, 1.0d / l);
					}
					for(String s : myListE.keySet()) {
						myList.put(s, 1.0d / l);
					}
				}
				if (myListC.size() > 0) {
					double l = myListC.size();
					for(String s : myListC.keySet()) {
						myList.put(s, 1.0d / l);
					}
				}
				
				 
			}

		} catch (Exception e) {
			LOG.error(e);
			e.printStackTrace();
		}
		return myList;
		
	}
	
}
