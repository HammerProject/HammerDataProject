package org.hammer.isabella.cc.util;

import java.util.ArrayList;
import java.util.SortedMap;

import org.hammer.isabella.cc.Token;
import org.hammer.isabella.query.AttributeValue;
import org.hammer.isabella.query.Edge;
import org.hammer.isabella.query.InstanceNode;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.LabelValueNode;
import org.hammer.isabella.query.NumberValueNode;
import org.hammer.isabella.query.QueryGraph;
import org.hammer.isabella.query.QuestionEdge;
import org.hammer.isabella.query.QuestionNode;
import org.hammer.isabella.query.RootNode;
import org.hammer.isabella.query.TextValueNode;
import org.hammer.isabella.query.ValueNode;

/**
 * Semantic Util
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Isabella
 *
 */
public class SemanticUtil {

	/**
	 * Set attribute value for where object
	 * 
	 * @param sl
	 * @return
	 */
	public static Edge SetAttrValueWhere(SortedMap<String, AttributeValue> sl,
			SortedMap<Integer, IsabellaError> errorList, Token t) {
		Edge edge = new Edge(sl.containsKey("label1") ? sl.get("label1").getLiteralValue() : "", t.beginLine, t.beginColumn);
		edge.setInstance(sl.containsKey("instance1") ? sl.get("instance1").getLiteralValue() : "");
		edge.setOperator(sl.containsKey("operator") ? sl.get("operator").getLiteralValue() : "");
		edge.setCondition(sl.containsKey("logicaloperator") ? sl.get("logicaloperator").getLiteralValue() : "");
		ValueNode valueNode = new TextValueNode("", t.beginLine, t.beginColumn);
		if (sl.containsKey("instance2") && sl.get("instance2").getLiteralValue() != null
				&& sl.get("instance2").getLiteralValue().trim().length() > 0) {
			valueNode = new LabelValueNode(sl.get("instance2").getLiteralValue(),
					sl.containsKey("label2") ? sl.get("label2").getLiteralValue() : "", t.beginLine, t.beginColumn);
		} else if (sl.containsKey("value") && sl.get("value").getLiteralValue() != null
				&& sl.get("value").getLiteralValue().trim().length() > 0) {
			valueNode = new TextValueNode(sl.get("value").getLiteralValue(), t.beginLine, t.beginColumn);
		} else if (sl.containsKey("value") && sl.get("value").getNumberValue() != null) {
			valueNode = new NumberValueNode(sl.get("value").getNumberValue().toString(), t.beginLine, t.beginColumn);
		} else if (sl.containsKey("value") && sl.get("value").getFloatValue() != null) {
			valueNode = new NumberValueNode(sl.get("value").getFloatValue().toString(), t.beginLine, t.beginColumn);
		}
		valueNode.eval(errorList, t.beginLine, t.beginColumn);
		edge.addChild(valueNode);
		edge.eval(errorList, t.beginLine, t.beginColumn);
		return edge;
	}
	
	/**
	 * Set attribute value for select object
	 * 
	 * @param sl
	 * @return
	 */
	public static QuestionEdge SetAttrValueSelect(SortedMap<String, AttributeValue> sl,
			SortedMap<Integer, IsabellaError> errorList, Token t) {
		QuestionEdge edge = new QuestionEdge(sl.containsKey("label") ? sl.get("label").getLiteralValue() : "", t.beginLine, t.beginColumn);
		edge.setInstance(sl.containsKey("instance") ? sl.get("instance").getLiteralValue() : "");
		edge.eval(errorList, t.beginLine, t.beginColumn);
		QuestionNode qN = new QuestionNode("?",0.0f,0.0f,0.0f, t.beginLine, t.beginColumn);
		edge.addChild(qN);
		return edge;
	}
	
	/**
	 * Set attribute value for from object
	 * 
	 * @param sl
	 * @return
	 */
	public static InstanceNode SetAttrValueFrom(SortedMap<Integer, IsabellaError> errorList, Token t) {
		InstanceNode node = new InstanceNode(t.image.substring(1,t.image.length()-1), t.beginLine, t.beginColumn);
		node.eval(errorList, t.beginLine, t.beginColumn);
		return node;
	}
	
	/**
	 * Build the graph of the query
	 * @param errorList
	 * @param qList
	 * @param iList
	 * @param wList
	 * @return
	 */
	public static QueryGraph Build(SortedMap<Integer, IsabellaError> errorList, ArrayList<QuestionEdge> qList, ArrayList<InstanceNode> iList, ArrayList<Edge> wList) {
		RootNode root = new RootNode();
		//create root and attach every instance from the from statement
		for(InstanceNode instance : iList) {
			root.addChild(instance);
		}
		//attach to instance, every label from the select statement
		for(QuestionEdge node: qList) {
			boolean findInstance = false;
			for(InstanceNode instanceNode : iList) {
				if(instanceNode.getName().equals(node.getInstance())) {
					findInstance = true;
					instanceNode.addChild(node);
					break;
				}
			}
			if(!findInstance) {
				errorList.put((node.getLine() * 1000) + 1, new IsabellaError(node.getLine(), node.getColumn(),
						"Instance code " + node.getInstance() + " not in instance list"));
			}
		}
		

		//attach to instance, every condition from the where statement

		for(Edge node: wList) {
			boolean findInstance = false;
			for(InstanceNode instanceNode : iList) {
				if(instanceNode.getName().equals(node.getInstance())) {
					findInstance = true;
					instanceNode.addChild(node);
					break;
				}
			}
			if(!findInstance) {
				errorList.put((node.getLine() * 1000) + 1, new IsabellaError(node.getLine(), node.getColumn(),
						"Instance code " + node.getInstance() + " not in instance list"));
			}
		}
		
		QueryGraph qG = new QueryGraph(root, qList, iList, wList);
		return qG;
	}

}
