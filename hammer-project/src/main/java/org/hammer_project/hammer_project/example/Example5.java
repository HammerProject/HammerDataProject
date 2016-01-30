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
 * Example 5 - Inquinamento
 * 
 *  PM10 - (µg/m³) --> pm10_media_annua_g_m3  (limite 40 µg/m³ media annua)
 *  Numero sanzioni ambientali --> num_sanz_ambientali
 *  PM2.5 - (µg/m³) --> pm2_5_media_annua_g_m3 (limite 25 µg/m³ media annua (dal 2015))
 *  NO2 - (µg/m³) --> no2_media_annua_g_m3 (limite 40 µg/m³ media annua)
 *  O3 - (µg/m³) --> o3_media_annua_g_m3 (informazione 180 µg/m³ media oraria)
 *  CO - (mg/m³) --> co_media_annua_mg_m3 (limite giornaliero 10 mg/m³ come MM8)
 *  SO2 - (µg/m³) --> so2_media_annua_g_m3 (125 µg/m³ da non superare per più di 3 giorni all’anno, per la vegetazione 20 µg/m³)
 *  C6H6 - (µg/m³) --> c6h6_media_annua_g_m3 (5 µg/m³ media annua)
 *  
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project
 *
 */
public class Example5 {

	public static void main(String[] args) throws Exception {
		ArrayList<BasicDBObject> docs = HammerUtils.GetFromFile("example/example_5/data.json");
		HashMap<String, BasicDBObject> newDoc = new HashMap<String, BasicDBObject>();

		for (BasicDBObject doc : docs) {

			// sanzioni x ambiente
			if ((doc.containsField("datasource_id")
					&& doc.getString("datasource_id").contains("REGIONE LOMBARDIA-qkux-6hb6"))) {
				BasicDBObject dbj = new BasicDBObject();
				dbj.put("provincia", doc.get("provincia").toString().toUpperCase());
				int num = Integer.parseInt(doc.get("num_sanz_ambientali") + "");
				dbj.put("num_sanz_ambientali", num);
				if (!newDoc.containsKey(doc.get("provincia").toString().toUpperCase() + "")) {
					newDoc.put(doc.get("provincia").toString().toUpperCase() + "", dbj);
				} else {
					if (newDoc.get(doc.get("provincia").toString().toUpperCase() + "").keySet()
							.contains("num_sanz_ambientali")) {
						int numOld = newDoc.get(doc.get("provincia").toString().toUpperCase() + "")
								.getInt("num_sanz_ambientali");
						num = num + numOld;
					}
					newDoc.get(doc.get("provincia").toString().toUpperCase() + "").put("num_sanz_ambientali", num);
				}
			}

			// qualità aria
			if ((doc.containsField("datasource_id")
					&& doc.getString("datasource_id").contains("REGIONE LOMBARDIA-x4dw-9y9x"))) {
				BasicDBObject dbj = new BasicDBObject();
				dbj.put("provincia",
						doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase());
				if (doc.keySet().contains("so2_media_annua_g_m3")) {
					float num = Float.parseFloat(doc.get("so2_media_annua_g_m3") + "");
					dbj.put("so2_media_annua_g_m3", num);
					if (!newDoc.containsKey(
							doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")) {
						newDoc.put(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "",
								dbj);
						
					} else {
						if (newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.keySet().contains("so2_media_annua_g_m3")) {
							int numOld = newDoc.get(
									doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase()
											+ "")
									.getInt("so2_media_annua_g_m3");
							num = (num + numOld) / 2;
						}
						newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.put("so2_media_annua_g_m3", num);
					}
				}
			}
			
			if ((doc.containsField("datasource_id")
					&& doc.getString("datasource_id").contains("REGIONE LOMBARDIA-x4dw-9y9x"))) {
				BasicDBObject dbj = new BasicDBObject();
				dbj.put("provincia",
						doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase());
				if (doc.keySet().contains("c6h6_media_annua_g_m3")) {
					float num = 0.0f;
					try {
						num = Float.parseFloat(doc.get("c6h6_media_annua_g_m3") + "");
					} catch (Exception e) {
						num = 0.0f;
					}
					dbj.put("c6h6_media_annua_g_m3", num);
					if (!newDoc.containsKey(
							doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")) {
						newDoc.put(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "",
								dbj);
						
					} else {
						if (newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.keySet().contains("c6h6_media_annua_g_m3")) {
							int numOld = newDoc.get(
									doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase()
											+ "")
									.getInt("c6h6_media_annua_g_m3");
							num = (num + numOld) / 2;
						}
						newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.put("c6h6_media_annua_g_m3", num);
					}
				}
			}
			
			
			if ((doc.containsField("datasource_id")
					&& doc.getString("datasource_id").contains("REGIONE LOMBARDIA-x4dw-9y9x"))) {
				BasicDBObject dbj = new BasicDBObject();
				dbj.put("provincia",
						doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase());
				if (doc.keySet().contains("pm2_5_media_annua_g_m3")) {
					float num = Float.parseFloat(doc.get("pm2_5_media_annua_g_m3") + "");
					dbj.put("pm2_5_media_annua_g_m3", num);
					if (!newDoc.containsKey(
							doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")) {
						newDoc.put(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "",
								dbj);
						
					} else {
						if (newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.keySet().contains("pm2_5_media_annua_g_m3")) {
							int numOld = newDoc.get(
									doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase()
											+ "")
									.getInt("pm2_5_media_annua_g_m3");
							num = (num + numOld) / 2;
						}
						newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.put("pm2_5_media_annua_g_m3", num);
					}
				}
			}
			
			if ((doc.containsField("datasource_id")
					&& doc.getString("datasource_id").contains("REGIONE LOMBARDIA-x4dw-9y9x"))) {
				BasicDBObject dbj = new BasicDBObject();
				dbj.put("provincia",
						doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase());
				if (doc.keySet().contains("pm10_media_annua_g_m3")) {
					float num = Float.parseFloat(doc.get("pm10_media_annua_g_m3") + "");
					dbj.put("pm10_media_annua_g_m3", num);
					if (!newDoc.containsKey(
							doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")) {
						newDoc.put(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "",
								dbj);
						
					} else {
						if (newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.keySet().contains("pm10_media_annua_g_m3")) {
							int numOld = newDoc.get(
									doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase()
											+ "")
									.getInt("pm10_media_annua_g_m3");
							num = (num + numOld) / 2;
						}
						newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.put("pm10_media_annua_g_m3", num);
					}
				}
			}
			
			
			if ((doc.containsField("datasource_id")
					&& doc.getString("datasource_id").contains("REGIONE LOMBARDIA-x4dw-9y9x"))) {
				BasicDBObject dbj = new BasicDBObject();
				dbj.put("provincia",
						doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase());
				if (doc.keySet().contains("o3_media_annua_g_m3")) {
					float num = Float.parseFloat(doc.get("o3_media_annua_g_m3") + "");
					dbj.put("o3_media_annua_g_m3", num);
					if (!newDoc.containsKey(
							doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")) {
						newDoc.put(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "",
								dbj);
						
					} else {
						if (newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.keySet().contains("o3_media_annua_g_m3")) {
							int numOld = newDoc.get(
									doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase()
											+ "")
									.getInt("o3_media_annua_g_m3");
							num = (num + numOld) / 2;
						}
						newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.put("o3_media_annua_g_m3", num);
					}
				}
			}
			
			
			if ((doc.containsField("datasource_id")
					&& doc.getString("datasource_id").contains("REGIONE LOMBARDIA-x4dw-9y9x"))) {
				BasicDBObject dbj = new BasicDBObject();
				dbj.put("provincia",
						doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase());
				if (doc.keySet().contains("no2_media_annua_g_m3")) {
					float num = Float.parseFloat(doc.get("no2_media_annua_g_m3") + "");
					dbj.put("no2_media_annua_g_m3", num);
					if (!newDoc.containsKey(
							doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")) {
						newDoc.put(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "",
								dbj);
						
					} else {
						if (newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.keySet().contains("no2_media_annua_g_m3")) {
							int numOld = newDoc.get(
									doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase()
											+ "")
									.getInt("no2_media_annua_g_m3");
							num = (num + numOld) / 2;
						}
						newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.put("no2_media_annua_g_m3", num);
					}
				}
			}
			
			
			
			if ((doc.containsField("datasource_id")
					&& doc.getString("datasource_id").contains("REGIONE LOMBARDIA-x4dw-9y9x"))) {
				BasicDBObject dbj = new BasicDBObject();
				dbj.put("provincia",
						doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase());
				if (doc.keySet().contains("no2_media_annua_g_m3")) {
					float num = Float.parseFloat(doc.get("no2_media_annua_g_m3") + "");
					dbj.put("no2_media_annua_g_m3", num);
					if (!newDoc.containsKey(
							doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")) {
						newDoc.put(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "",
								dbj);
						
					} else {
						if (newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.keySet().contains("no2_media_annua_g_m3")) {
							int numOld = newDoc.get(
									doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase()
											+ "")
									.getInt("no2_media_annua_g_m3");
							num = (num + numOld) / 2;
						}
						newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.put("no2_media_annua_g_m3", num);
					}
				}
			}
			
			
			
			if ((doc.containsField("datasource_id")
					&& doc.getString("datasource_id").contains("REGIONE LOMBARDIA-x4dw-9y9x"))) {
				BasicDBObject dbj = new BasicDBObject();
				dbj.put("provincia",
						doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase());
				if (doc.keySet().contains("co_media_annua_mg_m3")) {
					float num = Float.parseFloat(doc.get("co_media_annua_mg_m3") + "");
					dbj.put("co_media_annua_mg_m3", num);
					if (!newDoc.containsKey(
							doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")) {
						newDoc.put(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "",
								dbj);
						
					} else {
						if (newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.keySet().contains("co_media_annua_mg_m3")) {
							int numOld = newDoc.get(
									doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase()
											+ "")
									.getInt("co_media_annua_mg_m3");
							num = (num + numOld) / 2;
						}
						newDoc.get(
								doc.get("anagrafica_stazione_di_campionamento_provincia").toString().toUpperCase() + "")
								.put("co_media_annua_mg_m3", num);
					}
				}
			}
			
			
			
			

			// impianti a gasolio
			if ((doc.containsField("datasource_id")
					&& doc.getString("datasource_id").contains("REGIONE LOMBARDIA-rsg3-xhvk"))) {
				BasicDBObject dbj = new BasicDBObject();
				dbj.put("provincia", doc.get("provincia").toString().toUpperCase());
				int num = 1;
				dbj.put("num_abitazioni", num);
				if (!newDoc.containsKey(doc.get("provincia").toString().toUpperCase() + "")) {
					newDoc.put(doc.get("provincia").toString().toUpperCase() + "", dbj);
				} else {
					if (newDoc.get(doc.get("provincia").toString().toUpperCase() + "").keySet()
							.contains("num_abitazioni")) {
						int numOld = newDoc.get(doc.get("provincia").toString().toUpperCase() + "")
								.getInt("num_abitazioni");
						num = num + numOld;
					}
					newDoc.get(doc.get("provincia").toString().toUpperCase() + "").put("num_abitazioni", num);
				}
			}
			

			
		}

		List<BasicDBObject> list = SortMap(newDoc);
		try {
			FileOutputStream out = new FileOutputStream("example/example_5/results_5.json");
			int count = 0;
			int total = 0;
			out.write(new String("[").getBytes());
			for (BasicDBObject doc : list) {

				if (total > 0) {
					out.write(new String(",").getBytes());
				}
				out.write(doc.toString().getBytes());
				total++;
				/*
				 * if (total == 20) { break; }
				 */
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
				if (o1.getValue().keySet().contains("pm10_media_annua_g_m3")) {
					n1 = o1.getValue().getInt("pm10_media_annua_g_m3");
				}
				if (o2.getValue().keySet().contains("pm10_media_annua_g_m3")) {
					n2 = o2.getValue().getInt("pm10_media_annua_g_m3");
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
