package org.hammer.isabella.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * The query graph
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class QueryGraph {

	/**
	 * List of Question Node from "select" statement
	 */
	private List<QuestionEdge> questionNode = new ArrayList<QuestionEdge>();

	/**
	 * List of Instance Node from "from" statement
	 */
	private List<InstanceNode> instanceNode = new ArrayList<InstanceNode>();

	/**
	 * List of Instance Node from "where" statement
	 */
	private List<Edge> whereNode = new ArrayList<Edge>();

	/**
	 * Root Node
	 */
	private RootNode root = new RootNode();

	/**
	 * List of my KeyWords
	 */
	private List<String> keyWords = new ArrayList<String>();
	
	/**
	 * List of all my label
	 */
	private List<String> myLabels = new ArrayList<String>();
	
	
	public HashMap<String, Keyword> getIndex() {
		return index;
	}

	public void setIndex(HashMap<String, Keyword> index) {
		this.index = index;
	}

	/**
	 * List of all my label
	 */
	private HashMap<String, Keyword> index = new HashMap<String, Keyword>();

	/**
	 * Constuctor for the Query Graph
	 * 
	 * @param root
	 * @param qList
	 * @param iList
	 * @param wList
	 */
	public QueryGraph(RootNode root, ArrayList<QuestionEdge> qList, ArrayList<InstanceNode> iList, ArrayList<Edge> wList) {
		this.questionNode = qList;
		this.instanceNode = iList;
		this.whereNode = wList;
		this.root = root;
	}

	/**
	 * Print the Query Graph
	 */
	public void test() {
		this.root.test(0);
	}

	/**
	 * Return query condition list
	 * 
	 * @return
	 */
	public List<Edge> getQueryCondition() {
		return whereNode;
	}
	
	/**
	 * Return instance list
	 * 
	 * @return
	 */
	public List<InstanceNode> getInstanceNode() {
		return instanceNode;
	}

	@Override
	public int hashCode() {
		int result = 31 + this.root.hashCode();
		return result;
	}

	/**
	 * Update r-score
	 * 
	 * @param select
	 */
	private void updateScore(Node select) {
		ArrayList<Node> queue = new ArrayList<Node>();
		queue.add(select);
		queue.add(select.father);
		double vol = select.rScore();

		while (!queue.isEmpty()) {
			Node t = queue.get(0);
			queue.remove(0);
			int out = 1;
			if (t instanceof Edge) {
				out = 1;
			} else {
				ArrayList<String> labels = new ArrayList<String>();
				t.countLabels(labels);
				out = labels.size();
			}
			for (Node node : t.getChild()) {
				if (node != t.father()) {
					node.dec(vol / out / 2);
					if (!queue.contains(node)) {
						queue.add(node);
					}
				}
			}
		}
	}

	/**
	 * Update the reScore for each node
	 */
	private void updareReScore() {
		root.updareReScore(this.index);
	}
	
	/**
	 * Select label
	 */
	public void labelSelection() {
		this.updareReScore();
		this.test();
		System.out.println("--------- find labels -------");
		this.keyWords = new ArrayList<String>();

		Node k = root.valid(root);
		while (k != null) {
			System.out.println("---------" + k.getName() + " --!!! " + (k.rScore() + k.getiScore()));
			k.setSelected(true);
			if (!this.keyWords.contains(k.getName()) && k.getName() != "*" && k.getName().trim().length() > 2) {
				this.keyWords.add(k.getName());
			}
			this.updateScore(k);
			k = root.valid(root);
		}
		System.out.println("--------- print label -------");
		for (String keyWord : keyWords) {
			System.out.println(" ----> " + keyWord);
		}

	}
	
	/**
	 * Select label
	 */
	public void calculateMyLabels() {
		this.updareReScore();
		System.out.println("--------- find all labels -------");
		this.test();
		root.countLabels(this.myLabels);
		System.out.println("--------- print all labels -------");
		for (String label : myLabels) {
			System.out.println(" ----> " + label);
		}

	}

	/**
	 * Get the list of keyword (...;...;...)
	 * @return
	 */
	public String getKeyWords() {
		String t = "";
		for (String keyWord : keyWords) {
			t += keyWord + ";";
		}
		return t;

	}

	/**
	 * Return the question node
	 * @return
	 */
	public List<QuestionEdge> getQuestionNode() {
		return questionNode;
	}


	/**
	 * Get root
	 * @return
	 */
	public RootNode getRoot() {
		return root;
	}
	
	
	/**
	 * Get the list of field for join
	 * @return
	 */
	public String getJoinCondition() {
		String t = "";
		for(Edge edge : whereNode) {
			if(edge.getChild().get(0) instanceof LabelValueNode) {
				t += edge.getName() + ";";
			}
		}
		return t;

	}

	/**
	 * Get the list of all my labels
	 */
	public List<String> getMyLabelsAsList() {
		return myLabels;
	}
	
	/**
	 * Get the list of all my labels (...;...;...)
	 * @return
	 */
	public String getMyLabels() {
		String t = "";
		for (String label : myLabels) {
			t += label + ";";
		}
		return t;

	}

	

}
