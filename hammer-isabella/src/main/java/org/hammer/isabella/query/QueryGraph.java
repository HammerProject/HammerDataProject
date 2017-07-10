package org.hammer.isabella.query;

import java.io.Serializable;
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
public class QueryGraph implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4443478641451747940L;

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

	/**
	 * Set index
	 * @param index
	 */
	public void setIndex(HashMap<String, Keyword> index) {
		this.index = index;
	}

	/**
	 * List of all my label
	 */
	private HashMap<String, Keyword> index = new HashMap<String, Keyword>();

	/**
	 * List of weight for where
	 */
	private HashMap<String, Float> wWhere = new HashMap<String, Float>();
	
	/**
	 * Weight (total) of where
	 */
	private float weightWhere = 0.f;
	
	
	public float getWeightWhere() {
		return weightWhere;
	}

	/**
	 * Word Net Home
	 */
	private String wnHome = "";
	
	public String getWnHome() {
		return wnHome;
	}

	public void setWnHome(String wnHome) {
		this.wnHome = wnHome;
	}

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
		// calc wWhere
		for(Edge en: whereNode) {
			float pw = 0.0f;
			float scw = 0.0f;
			int foundInSelection = 0;
			int foundInProiection = 0;
			for(Edge temp: whereNode) {
				if(temp.getName().equals(en.getName())) {
					foundInSelection++;
				}
			}
			for(QuestionEdge temp: questionNode) {
				if(temp.getName().equals(en.getName())) {
					foundInProiection++;
				}
			}
			if(en.getCondition().equals("AND")) {
				scw = 1.0f;
			} else if (foundInSelection == 1) {
				scw = 1.0f;
			} else {
				scw = 0.7f;
			}
			if (foundInProiection == 1) {
				pw = 1.0f;
			} else {
				pw = 0.0f;
			}
			wWhere.put(en.getName(), (pw >= scw) ? pw : scw);
			weightWhere+= (pw >= scw) ? pw : scw;
		}
		
	}

	/**
	 * Return the list with weight
	 * @return
	 */
	public HashMap<String, Float> getwWhere() {
		return wWhere;
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
	private void updareReScore(boolean sub) {
		root.updareReScore(this.index, sub);
	}
	
	/**
	 * Select label
	 */
	public void labelSelection() {
		this.calculateSimilarity();
		this.updareReScore(true);
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
		this.updareReScore(false);
		System.out.println("--------- find all labels -------");
		this.test();
		root.countLabels(this.myLabels);
		System.out.println("--------- print all labels -------");
		for (String label : myLabels) {
			System.out.println(" ----> " + label);
		}

	}
	
	/**
	 * Select similarity
	 */
	public void calculateSimilarity() {
		root.calcSimilaritySet(this.index, this.wnHome);
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
			t += label.toLowerCase() + ";";
		}
		return t;

	}

	public void newQ(ArrayList<String[]> arrayList) {
		root.newQ(arrayList);
	}

	

}
