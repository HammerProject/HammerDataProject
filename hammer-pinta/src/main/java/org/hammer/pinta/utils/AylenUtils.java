package org.hammer.pinta.utils;

import com.aylien.textapi.TextAPIClient;
import com.aylien.textapi.parameters.*;
import com.aylien.textapi.responses.*;


public class AylenUtils {

	  public static void main(String[] args) throws Exception {
	    TextAPIClient client = new TextAPIClient("21773429", "1fcb74d098fe70de88a1bdc28c380a1c");
	    
	    
	    RelatedParams params = new RelatedParams("machine learning", 20);
	    Related related = client.related(params);
	    for (Phrase phrase: related.getPhrases()) {
	      System.out.println(phrase.getPhrase());
	    }
	  }
	
	
}
