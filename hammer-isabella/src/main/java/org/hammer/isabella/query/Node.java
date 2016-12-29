package org.hammer.isabella.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hammer.isabella.fuzzy.JaroWinkler;
import org.hammer.isabella.fuzzy.WordNetUtils;

/**
 * Node
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class Node implements Leaf, IDataType, Serializable {

	private static final Log LOG = LogFactory.getLog(Node.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 2082275786927456129L;

	/**
	 * The synset
	 */
	private List<Keyword> synSet = new ArrayList<Keyword>();

	public List<Keyword> getSynSet() {
		return synSet;
	}
	
	/**
	 * The similatiry set
	 */
	private List<Keyword> similatirySet = new ArrayList<Keyword>();

	public List<Keyword> getSimilatirySet() {
		return similatirySet;
	}

	public String getSimName() {
		return simName;
	}

	/**
	 * The string most similarity
	 */
	private String simName = null;

	Comparator<Keyword> cmp = new Comparator<Keyword>() {
		public int compare(Keyword o1, Keyword o2) {
			return (o1.getSimilarity() < o2.getSimilarity()) ? 1 : ((o1.getSimilarity() > o2.getSimilarity()) ? -1 : 0);
		}
	};

	/**
	 * Calc similarity set for this name
	 * 
	 * @param index
	 */
	public void calcSimilaritySet(HashMap<String, Keyword> index, String wnHome) {
		if (this.simName == null && !this.name.equals("Q") && !this.name.equals("?") && !this.name.equals("*")) {
			for (String s : index.keySet()) {
				double sim = JaroWinkler.Apply(this.name.toLowerCase(), s.toLowerCase());
				// avg the value of sim with the value of re
				// we want to give more importance to terms that are more
				// representative of our index
				double re = index.get(s).getReScore();
				sim = (sim + re) / 2.0d;
				
				if (sim > 0) {

					
					Keyword k = index.get(s).clone();
					k.setSimilarity(sim);
					this.similatirySet.add(k);
					this.similatirySet.sort(cmp);
					this.simName = this.similatirySet.get(0).getKeyword();
					
				}
			}
			
			Map<String, Double> mySynSet = WordNetUtils.MySynset(wnHome, this.name.toLowerCase());
			
			
			for (String s : mySynSet.keySet()) {
				if (index.containsKey(s)) {
					// a term of synset has the same re of the original term
					double re = index.get(s).getReScore();
					double sim = (mySynSet.get(s) + re) / 2.0d;
					Keyword k = index.get(s).clone();
					k.setSimilarity(sim);
					this.synSet.add(k);
					this.synSet.sort(cmp);
					
					// if find a good synset, we can substitute the sinName by Jaro with synset by WordNet
					this.simName = this.synSet.get(0).getKeyword();
				}
			}
			
			System.out.println("-------------------------------------------------");
			System.out.println(this.name.toString());
			System.out.println(this.simName.toString());
			System.out.println("-------------------------------------------------");
			
			if (this.father != null) {
				this.father.calcSimilaritySet(index, wnHome);
			}

		}

		for (Node node : getChild()) {
			node.calcSimilaritySet(index, wnHome);
		}
	}

	/**
	 * Update reScore
	 * 
	 * @param index
	 */
	public void updareReScore(HashMap<String, Keyword> index) {
		if (index.containsKey(this.getName().toLowerCase())) {
			// System.out.println(" found !!! " +
			// index.get(this.getName().toLowerCase()).toString());
			this.reScore = index.get(this.getName().toLowerCase()).getReScore();
		} else if (this.simName != null) {
			System.out.println(" not found !!! " + this.simName.toString());
			this.reScore = index.get(this.simName.toLowerCase()).getReScore();
			// take the value of most sim terms and replace the label of the query
			// in other terms we modify the original graph with a graph "fitted" with 
			// our index
			this.name = this.simName.toLowerCase();
		} else {
			LOG.debug(" not found !!! " + this.getName().toString());
			this.reScore = 0.0f;
			this.riScore = 0.0f;
		}
		for (Node node : getChild()) {
			node.updareReScore(index);
		}
	}

	/**
	 * True if the node is selected for the label list
	 */
	private boolean selected = false;

	/**
	 * The list of my child
	 */
	private List<Node> child = new ArrayList<Node>();

	/**
	 * My i-score
	 */
	private float iScore = 0.0f;

	/**
	 * My ri-score
	 */
	private float riScore = 0.0f;

	/**
	 * My re-score
	 */
	private double reScore = 0.0f;

	/**
	 * Line from the source code
	 */
	private int line = 0;

	/**
	 * Column from the source code
	 */
	private int column = 0;

	/**
	 * Name
	 */
	private String name = "";

	/**
	 * My father
	 */
	protected Node father = null;

	/**
	 * Get my father
	 */
	public Node father() {
		return father;
	}

	/**
	 * Return my name
	 * 
	 * @return
	 */
	public String getName() {
		return name;
	}

	/**
	 * Return the list of my child
	 * 
	 * @return
	 */
	public List<Node> getChild() {
		return child;
	}

	/**
	 * Add a child
	 * 
	 * @param node
	 */
	public void addChild(Node node) {
		node.father = this;
		this.child.add(node);
	}

	/**
	 * Return my informative score
	 * 
	 * @return
	 */
	public float getiScore() {
		return iScore;
	}

	/**
	 * Return my representive in score
	 * 
	 * @return
	 */
	public float getriScore() {
		return riScore;
	}

	/**
	 * Return my representive ext score
	 * 
	 * @return
	 */
	public double getreScore() {
		return reScore;
	}

	/**
	 * Consume my informative
	 * 
	 * @param value
	 */
	public void dec(double value) {
		iScore -= value;
	}

	/**
	 * Create a Node
	 * 
	 * @param name
	 * @param iScore
	 * @param rScore
	 * @param line
	 * @param column
	 */
	public Node(String name, float iScore, float riScore, float reScore, int line, int column) {
		this.name = name;
		this.iScore = iScore;
		this.riScore = riScore;
		this.reScore = reScore;
		this.line = line;
		this.column = column;
		this.child = new ArrayList<Node>();

		if (this.name == "*") {
			this.iScore = 0.0f;
			this.riScore = 0.0f;
			this.reScore = 0.0f;
		} else if (this.name == "Q") {
			this.iScore = 0.0f;
			this.riScore = 0.0f;
			this.reScore = 0.0f;
		} else if (this.name == "?") {
			this.iScore = 0.0f;
			this.riScore = 0.0f;
			this.reScore = 0.0f;
		}
	}

	/**
	 * Get my line on source file
	 * 
	 * @return
	 */
	public int getLine() {
		return line;
	}

	/**
	 * Get my column on source file
	 * 
	 * @return
	 */
	public int getColumn() {
		return column;
	}

	/**
	 * Print the Node
	 * 
	 * !!! initialize all data
	 */
	public void test(int level) {
		int l = level + 1;
		this.selected = false;
		for (int i = 0; i < level; i++) {
			System.out.print("-");
		}
		System.out.println("-> " + this.name + " (" + iScore + ", " + riScore + "," + reScore + ")");
		for (Node node : getChild()) {
			node.test(l);
		}
	}

	/**
	 * Count the labels on the graph
	 * 
	 * @param labels
	 */
	public void countLabels(List<String> labels) {
		if (!labels.contains(this.name) && this.name != "*" && this.name != "?" && !this.name.equals("Q")) {
			labels.add(this.name);
		}
		if (this.father != null) {
			if (!labels.contains(this.father.getName()) && this.name != "*" && this.name != "?"
					&& !this.name.equals("Q")) {
				labels.add(this.father.getName());
			}
		}
		for (Node node : getChild()) {
			node.countLabels(labels);
		}
	}

	/**
	 * Return a valid node (i + r > 1)
	 * 
	 * @param maxNode
	 * @return
	 */
	public Node valid(Node maxNode) {
		if (maxNode == null)
			maxNode = this;
		if (!this.selected) {
			if ((this.iScore + this.rScore()) >= (maxNode.iScore + maxNode.rScore())) {
				maxNode = this;
			}
		}
		for (Node node : getChild()) {
			maxNode = node.valid(maxNode);
		}
		if (maxNode != null && ((maxNode.iScore + maxNode.rScore()) <= 1.0f))
			return null;
		return maxNode;
	}

	/**
	 * Get My R Score
	 * 
	 * @return
	 */
	public double rScore() {
		return ((riScore + reScore) / 2.0d);
	}

	/**
	 * Node already select for the label list (=true)
	 * 
	 * @return
	 */
	public boolean isSelected() {
		return selected;
	}

	/**
	 * Set
	 * 
	 * @param pass
	 */
	public void setSelected(boolean selected) {
		this.selected = selected;
	}

	@Override
	public int hashCode() {
		int result = this.name != null ? this.name.hashCode() : 0;
		for (Node node : getChild()) {
			result = 31 * result + node.hashCode();
		}
		return result;
	}

	@Override
	public void eval(SortedMap<Integer, IsabellaError> errorList, int line, int column) {
		if (name == null || name.trim().length() <= 0) {
			errorList.put((line * 1000) + 1,
					new IsabellaError(line, column, "Name code non defined; \"name\" : \"xxxxx\""));
		}
	}
	

	public void newQ(ArrayList<String[]> arrayList) {
		for(String[] k : arrayList) {
			if(k[0].equals(this.name)) {
				this.name = k[1];
			}
		}
		for (Node node : getChild()) {
			node.newQ(arrayList);
		}
		
	}
}
