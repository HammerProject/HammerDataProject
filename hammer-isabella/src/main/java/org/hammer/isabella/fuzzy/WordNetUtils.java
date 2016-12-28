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
	public static Map<String, String> GetSynset(String wnhome, String term) throws IOException {
		Map<String, String> mylist = new HashMap<String, String>();
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
						mylist.put(w.getLemma().replaceAll("_",""),term);
						
					}
			}
			
		} catch (Exception e) {
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
	public static Map<String, String> MySynset(String wnhome, String term) {
		Map<String, String> myList = new HashMap<String, String>();
		try {
			term = term.replaceAll("[^a-zA-Z]", "").toLowerCase();
			myList = WordNetUtils.GetSynset(wnhome, term);
			for(int i = 3; i < term.length(); i++) {
				String termS = term.substring(0, i);
				String termE = term.substring(i, term.length());
				String termC = termS + "_" + termE;
				
				Map<String, String> myListS = new HashMap<String, String>();
				Map<String, String> myListE = new HashMap<String, String>();
				Map<String, String> myListC = new HashMap<String, String>();

				if (termS.length() >= 3) { myListS = WordNetUtils.GetSynset(wnhome, termS); }
				if (termE.length() >= 3) { myListE = WordNetUtils.GetSynset(wnhome, termE); }
				if (termC.length() >= 3) { myListC = WordNetUtils.GetSynset(wnhome, termC); }
				
				if (myListS.size() > 0 && myListE.size() > 0) {
					myList.putAll(myListS);
					myList.putAll(myListE);
				}
				myList.putAll(myListC);
				
				 
			}
			if(LOG.isDebugEnabled()) {
				for(String t : myList.keySet()) {
					System.out.println(t);
				}
			}
		} catch (Exception e) {
			LOG.error(e);
			e.printStackTrace();
		}
		return myList;
		
	}
	
}
