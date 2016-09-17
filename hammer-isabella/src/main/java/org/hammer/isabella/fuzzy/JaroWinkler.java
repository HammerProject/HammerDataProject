package org.hammer.isabella.fuzzy;

import java.util.ArrayList;
import java.util.StringTokenizer;


/**
 * Jaro Winkler similarity algorithm
 * from apache common text similarity
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 */
public class JaroWinkler {
    /**
     * Default prefix for Jaro Winkler
     */
    private static final int LEN_LIMIT = 4;
    

    /**
     * Find the Jaro Winkler Distance which indicates the similarity score
     * between two CharSequences.
     * 
     * {talendTypes} String
     * 
     * {Category} User Defined
     * 
     * {param} string("world") s1: first string
     * {param} string("world") s2: second string
     * 
     * {example} Apply("world","check") # hello world !.
     */
    public static float ApplyToTextByList(String s1, String s2) {
    	float f = 0.0f;
    	StringTokenizer st = new StringTokenizer(s2, ",");
    	int count = st.countTokens();
    	while (st.hasMoreElements()) {
    		String word = st.nextToken();
    		float fTemp = ApplyToText(s1, word);
    		f += fTemp;
    		if(fTemp >= 0.99f) {
    			return 1.0f;
    		}
    		
    	}
    	f = f / count;
    	return f;
    }
    
	/**
     * Find the Jaro Winkler Distance which indicates the similarity score
     * between two CharSequences.
     * 
     * {talendTypes} String
     * 
     * {Category} User Defined
     * 
     * {param} string("world") s1: first string
     * {param} string("world") s2: second string
     * 
     * {example} Apply("world","check") # hello world !.
     */
    public static float ApplyToText(String s1, String s2) {
    	s1 = s1.toLowerCase();
    	s2 = s2.toLowerCase();
    	ArrayList<String> ngram2 = NGram.ngram2(s1);
    	if(ngram2.size() == 0) {
    		return 0;
    	}
    	float f = 0.0f;
    	for(String term : ngram2) {
    		float fLocal = Apply(term, s2);
    		f += fLocal;
    		if (fLocal >= 0.99f) {
    			return 1.0f;
    		}
    	}
    	f = f / ngram2.size();
    	return f;
    }
    
    /**
     * Find the Jaro Winkler Distance which indicates the similarity score
     * between two CharSequences.
     *
     * <pre>
     * "frog", "fog"       = 0.93
     * "fly", "ant"        = 0.0
     * "elephant", "hippo" = 0.44
     * </pre>
     *
     * @param left the first String, must not be null
     * @param right the second String, must not be null
     * @return result distance
     * @throws IllegalArgumentException if either String input {@code null}
     */
    public static float Apply(String s1, String s2) {
    	CharSequence left = s1;
    	CharSequence right = s2;
        final double defaultScalingFactor = 0.1;
        final double percentageRoundValue = 100.0;
        if (left == null || right == null) {
            throw new IllegalArgumentException("Strings must not be null");
        }
        final double jaro = Score(left, right);
        final int cl = CommonPrefixLength(left, right);
        final double matchScore = Math.round((jaro + defaultScalingFactor
                * cl * (1.0 - jaro)) * percentageRoundValue) / percentageRoundValue;
        return (new Float(matchScore)).floatValue();
    }
    
    /**
     * Calculates the number of characters from the beginning of the strings
     * that match exactly one-to-one, up to a maximum of four (4) characters.
     *
     * @param first The first string.
     * @param second The second string.
     * @return A number between 0 and 4.
     */
    private static int CommonPrefixLength(final CharSequence first,
            final CharSequence second) {
        final int result = GetCommonPrefix(first.toString(), second.toString())
                .length();
        // Limit the result to 4.
        return result > LEN_LIMIT ? LEN_LIMIT : result;
    }
    
    /**
     * Compares all Strings in an array and returns the initial sequence of
     * characters that is common to all of them.
     *
     * <p>
     * For example,
     * <code>getCommonPrefix(new String[] {"i am a machine", "i am a robot"}) -&gt; "i am a "</code>
     * </p>
     *
     *
     * @param strs array of String objects, entries may be null
     * @return the initial sequence of characters that are common to all Strings
     *         in the array; empty String if the array is null, the elements are
     *         all null or if there is no common prefix.
     */
    public static String GetCommonPrefix(final String... strs) {
        if (strs == null || strs.length == 0) {
            return "";
        }
        final int smallestIndexOfDiff = IndexOfDifference(strs);
        if (smallestIndexOfDiff == -1) {
            // all strings were identical
            if (strs[0] == null) {
                return "";
            }
            return strs[0];
        } else if (smallestIndexOfDiff == 0) {
            // there were no common initial characters
            return "";
        } else {
            // we found a common initial character sequence
            return strs[0].substring(0, smallestIndexOfDiff);
        }
    }
    
    /**
     * This method returns the Jaro-Winkler score for string matching.
     *
     * @param first the first string to be matched
     * @param second the second string to be machted
     * @return matching score without scaling factor impact
     */
    private static double Score(final CharSequence first, final CharSequence second) {
        String shorter;
        String longer;
        // Determine which String is longer.
        if (first.length() > second.length()) {
            longer = first.toString().toLowerCase();
            shorter = second.toString().toLowerCase();
        } else {
            longer = second.toString().toLowerCase();
            shorter = first.toString().toLowerCase();
        }
        // Calculate the half length() distance of the shorter String.
        final int halflength = shorter.length() / 2 + 1;
        // Find the set of matching characters between the shorter and longer
        // strings. Note that
        // the set of matching characters may be different depending on the
        // order of the strings.
        final String m1 = MatchingCharacter(shorter, longer,
                halflength);
        final String m2 = MatchingCharacter(longer, shorter,
                halflength);
        // If one or both of the sets of common characters is empty, then
        // there is no similarity between the two strings.
        if (m1.length() == 0 || m2.length() == 0) {
            return 0.0;
        }
        // If the set of common characters is not the same size, then
        // there is no similarity between the two strings, either.
        if (m1.length() != m2.length()) {
            return 0.0;
        }
        // Calculate the number of transposition between the two sets
        // of common characters.
        final int transpositions = Transpositions(m1, m2);
        final double defaultDenominator = 3.0;
        // Calculate the distance.
        final double dist = (m1.length() / ((double) shorter.length())
                + m2.length() / ((double) longer.length()) + (m1.length() - transpositions)
                / ((double) m1.length())) / defaultDenominator;
        return dist;
    }
    
    /**
     * Calculates the number of transposition between two strings.
     *
     * @param first The first string.
     * @param second The second string.
     * @return The number of transposition between the two strings.
     */
    private static int Transpositions(final CharSequence first, final CharSequence second) {
        int transpositions = 0;
        for (int i = 0; i < first.length(); i++) {
            if (first.charAt(i) != second.charAt(i)) {
                transpositions++;
            }
        }
        return transpositions / 2;
    }
    
    /**
     * Compares all CharSequences in an array and returns the index at which the
     * CharSequences begin to differ.
     *
     * <p>
     * For example,
     * <code>indexOfDifference(new String[] {"i am a machine", "i am a robot"}) -&gt; 7</code>
     * </p>
     *
     */
    private static int IndexOfDifference(final CharSequence... css) {
        if (css == null || css.length <= 1) {
            return -1;
        }
        boolean anyStringNull = false;
        boolean allStringsNull = true;
        final int arrayLen = css.length;
        int shortestStrLen = Integer.MAX_VALUE;
        int longestStrLen = 0;
        // find the min and max string lengths; this avoids checking to make
        // sure we are not exceeding the length of the string each time through
        // the bottom loop.
        for (int i = 0; i < arrayLen; i++) {
            if (css[i] == null) {
                anyStringNull = true;
                shortestStrLen = 0;
            } else {
                allStringsNull = false;
                shortestStrLen = Math.min(css[i].length(), shortestStrLen);
                longestStrLen = Math.max(css[i].length(), longestStrLen);
            }
        }
        // handle lists containing all nulls or all empty strings
        if (allStringsNull || longestStrLen == 0 && !anyStringNull) {
            return -1;
        }
        // handle lists containing some nulls or some empty strings
        if (shortestStrLen == 0) {
            return 0;
        }
        // find the position with the first difference across all strings
        int firstDiff = -1;
        for (int stringPos = 0; stringPos < shortestStrLen; stringPos++) {
            final char comparisonChar = css[0].charAt(stringPos);
            for (int arrayPos = 1; arrayPos < arrayLen; arrayPos++) {
                if (css[arrayPos].charAt(stringPos) != comparisonChar) {
                    firstDiff = stringPos;
                    break;
                }
            }
            if (firstDiff != -1) {
                break;
            }
        }
        if (firstDiff == -1 && shortestStrLen != longestStrLen) {
            // we compared all of the characters up to the length of the
            // shortest string and didn't find a match, but the string lengths
            // vary, so return the length of the shortest string.
            return shortestStrLen;
        }
        return firstDiff;
    }
    
    /**
     * Search matching characters between two strings and return H
     * where
     * H = {char in common between first-string and second-string}
     * two char is in common if the respective positions is the same or the same with a limit of X-char
     *
     *
     * @param first
     * @param second
     * @param limit
     * @return H char
     */
    private static String MatchingCharacter(CharSequence first, CharSequence second, int limit) {
        final StringBuilder common = new StringBuilder();
        final StringBuilder copy = new StringBuilder(second);
        for (int i = 0; i < first.length(); i++) {
            final char ch = first.charAt(i);
            boolean found = false;
            for (int j = Math.max(0, i - limit); !found
                    && j < Math.min(i + limit, second.length()); j++) {
                if (copy.charAt(j) == ch) {
                    found = true;
                    common.append(ch);
                    copy.setCharAt(j, '*'); // delete the char
                }
            }
        }
        return common.toString();
    }
    
    
	public static void main( String[] args )
    {
		System.out.println(JaroWinkler.ApplyToTextByList("till ios10","ios10, sistema"));
    
    }
}
