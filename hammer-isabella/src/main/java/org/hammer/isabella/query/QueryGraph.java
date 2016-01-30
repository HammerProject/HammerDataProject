package org.hammer.isabella.query;

import java.util.ArrayList;
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
	private List<Edge> questionNode = new ArrayList<Edge>();

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
	 * Constuctor for the Query Graph
	 * 
	 * @param root
	 * @param qList
	 * @param iList
	 * @param wList
	 */
	public QueryGraph(RootNode root, ArrayList<Edge> qList, ArrayList<InstanceNode> iList, ArrayList<Edge> wList) {
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
	 * Update informative score
	 * 
	 * @param select
	 */
	private void updateIScore(Node select) {
		ArrayList<Node> queue = new ArrayList<Node>();
		queue.add(select);
		queue.add(select.father);
		float vol = select.getrScore();

		while (!queue.isEmpty()) {
			Node t = queue.get(0);
			queue.remove(0);
			int out = 1;
			if (t instanceof Edge) {
				out = 1;
			} else {
				ArrayList<String> labels = new ArrayList<String>();
				select.countLabels(labels);
				out = labels.size();
			}
			for (Node node : t.getChild()) {
				if (node != t.father()) {
					node.decI(vol / out / 2);
					if (!queue.contains(node)) {
						queue.add(node);
					}
				}
			}
		}
	}

	/**
	 * Select label
	 */
	public void labelSelection() {
		System.out.println("--------- find label -------");
		this.keyWords = new ArrayList<String>();

		Node k = root.valid(root);
		while (k != null) {
			System.out.println("---------" + k.getName() + " --!!! " + (k.getiScore() + k.getrScore()));
			k.setSelected(true);
			if (!this.keyWords.contains(k.getName()) && k.getName() != "*" && k.getName().trim().length() > 2) {
				this.keyWords.add(k.getName());
			}
			this.updateIScore(k);
			k = root.valid(root);
		}
		System.out.println("--------- print label -------");
		for (String keyWord : keyWords) {
			System.out.println(" ----> " + keyWord);
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
	public List<Edge> getQuestionNode() {
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

}
