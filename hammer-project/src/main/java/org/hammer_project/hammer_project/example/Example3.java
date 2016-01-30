package org.hammer_project.hammer_project.example;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hammer_project.hammer_project.util.HammerUtils;

import com.mongodb.BasicDBObject;

/**
 * Example 5 - Indicatori ambientali
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project
 *
 */
public class Example3 {

	public static void main(String[] args) throws Exception {
		ArrayList<BasicDBObject> docs = HammerUtils.GetFromFile("example/example_5/data.json");
		HashMap<String, BasicDBObject> newDoc = new HashMap<String, BasicDBObject>();

		for (BasicDBObject doc : docs) {
			if ((doc.containsField("datasource_id")
					&& doc.get("datasource_id").equals("REGIONE LOMBARDIA-tma4-vn2u"))) {
				BasicDBObject dbj = new BasicDBObject();
				dbj.put("comune", doc.get("comune").toString().toUpperCase());
				int numFamglie = Integer.parseInt(doc.get("numero_di_famiglie") + "");
				dbj.put("numero_di_famiglie", numFamglie);
				if (!newDoc.containsKey(doc.get("comune").toString().toUpperCase() + "")) {
					newDoc.put(doc.get("comune").toString().toUpperCase() + "", dbj);
				} else {
					newDoc.get(doc.get("comune").toString().toUpperCase() + "").put("numero_di_famiglie", numFamglie);
				}
			}

			if ((doc.containsField("datasource_id") && doc.get("datasource_id").equals("REGIONE LOMBARDIA-rsg3-xhvk")) // cened
			) {
				BasicDBObject dbj = new BasicDBObject();
				dbj.put("comune", doc.get("comune").toString().toUpperCase());
				
				if (!newDoc.containsKey(doc.get("comune").toString().toUpperCase() + "")) {
					dbj.put("num_abitazioni", 1);
					newDoc.put(doc.get("comune").toString().toUpperCase() + "", dbj);
				} else {
					if (newDoc.get(doc.get("comune") + "").keySet().contains("num_abitazioni")) {
						int num = newDoc.get(doc.get("comune") + "").getInt("num_abitazioni");
						dbj.put("num_abitazioni", num++);
						newDoc.get(doc.get("comune").toString().toUpperCase() + "").remove("num_abitazioni");
						newDoc.get(doc.get("comune").toString().toUpperCase() + "").put("num_abitazioni", num++);
					} else {
						newDoc.get(doc.get("comune").toString().toUpperCase() + "").put("num_abitazioni", 1);
					}
				}
			}

		}

		List<BasicDBObject> list = SortMap(newDoc);
		try {
			FileOutputStream out = new FileOutputStream("example/example_3/results_3.json");
			int count = 0;
			int total = 0;
			out.write(new String("[").getBytes());
			for (BasicDBObject doc : list) {

				if (total > 0) {
					out.write(new String(",").getBytes());
				}
				out.write(doc.toString().getBytes());
				total++;
				if (total == 20) {
					break;
				}
				count++;

			}
			out.write(new String("]").getBytes());
			out.close();
			System.out.println("Totale scritti " + total);
			System.out.println("Totale letti " + docs.size());
			System.out.println("Totale elaborati " + count);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static List<BasicDBObject> SortMap(Map<String, BasicDBObject> unsortMap) {
		List<Map.Entry<String, BasicDBObject>> list = new LinkedList<Map.Entry<String, BasicDBObject>>(
				unsortMap.entrySet());

		Collections.sort(list, new Comparator<Map.Entry<String, BasicDBObject>>() {
			public int compare(Map.Entry<String, BasicDBObject> o1, Map.Entry<String, BasicDBObject> o2) {
				Integer n1 = 1;
				Integer n2 = 0;
				if (o1.getValue().keySet().contains("num_abitazioni")) {
					n1 = o1.getValue().getInt("num_abitazioni");
				}
				if (o2.getValue().keySet().contains("num_abitazioni")) {
					n2 = o2.getValue().getInt("num_abitazioni");
				}
				if (n1 < n2)
					  return 1;
					else if (n1 >= n2)
					  return -1;
					else
					  return 0;
			}
		});

		List<BasicDBObject> sortedMap = new ArrayList<BasicDBObject>();
		for (Iterator<Map.Entry<String, BasicDBObject>> it = list.iterator(); it.hasNext();) {
			Map.Entry<String, BasicDBObject> entry = it.next();
			sortedMap.add(entry.getValue());
		}
		return sortedMap;
	}

}
