package org.hammer_project.hammer_project.example;

import java.io.FileOutputStream;
import java.util.ArrayList;

import org.hammer_project.hammer_project.util.HammerUtils;

import com.mongodb.BasicDBObject;

/**
 * Example 4
 * 
 * 
 * Mappa con musei e impianti sportivi in provincia di Bergamo abilitati ai disabili,
 * mostrare le strutture ricettive nelle vicinanze
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project
 *
 */
public class Example4 {

	public static void main(String[] args) throws Exception {
		ArrayList<BasicDBObject> docs = HammerUtils.GetFromFile("example/example_4/test.json");
		
		
		
		
		
		for (BasicDBObject doc : docs) {
			if (doc.containsField("letti") || // albergo
					doc.containsField("denominazione_struttura") || // albergo
					doc.containsField("nome_agriturismo") || // agriturismo
					(doc.containsField("sede_accessibile_ai_disabili") &&
							(doc.get("sede_accessibile_ai_disabili").equals("TOTALMENTE") || doc.get("sede_accessibile_ai_disabili").equals("PARZIALMENTE"))) || // museo
					( doc.containsField("fruibilita_disabili") && doc.get("fruibilita_disabili").equals("1")) // impianto sportivo
			) {

				String indirizzo = "";
				if (doc.keySet().contains("indirizzo")) {
					indirizzo += doc.getString("indirizzo") + ", ";
				}
				if (doc.keySet().contains("indirizzo_localita")) {
					indirizzo = doc.getString("indirizzo_localita") + ", ";
				}
				if (doc.keySet().contains("sede_indirizzo")) {
					indirizzo = doc.getString("sede_indirizzo") + ", ";
				}
				String comune = "";
				if (doc.keySet().contains("comune")) {
					indirizzo += doc.getString("comune");
					comune = doc.getString("comune");
				}
				if (doc.keySet().contains("sede_comune")) {
					indirizzo = doc.getString("sede_indirizzo") + ", " + doc.getString("sede_comune");
					comune = doc.getString("sede_comune");
				}
				BasicDBObject pos = new BasicDBObject();
				if (doc.keySet().contains("location")) {
					pos.append("lat", ((BasicDBObject) doc.get("location")).get("latitude"));
					pos.append("lon", ((BasicDBObject) doc.get("location")).get("longitude"));
				} else if (doc.keySet().contains("lng")) {
					pos.append("lat", doc.get("lat"));
					pos.append("lon", doc.get("lng"));
				} else if (doc.keySet().contains("wgs84_y")) {
					pos.append("lat", doc.get("wgs84_x"));
					pos.append("lon", doc.get("wgs84_y"));
				} else if (doc.keySet().contains("geo_x")) {
					pos.append("lat", doc.get("geo_x"));
					pos.append("lon", doc.get("geo_y"));
				} else {
					pos = HammerUtils.Google_GeoCoding(indirizzo.trim());
					if (pos == null || !pos.keySet().contains("lat")) {
						pos = HammerUtils.Google_GeoCoding(comune.trim());
					}
					//Thread.sleep(5000);
				}

				doc.append("indirizzo_new", indirizzo);
				doc.append("pos", pos);
				
			}
			
		}

		try {
			FileOutputStream out = new FileOutputStream("example/example_4/results_4.json");
			int count = 0;
			int total = 0;
			out.write(new String("[").getBytes());
			for (BasicDBObject doc : docs) {
				if (doc.containsField("letti") || // albergo
						doc.containsField("denominazione_struttura") || // albergo
						doc.containsField("nome_agriturismo") || // agriturismo
						(doc.containsField("sede_accessibile_ai_disabili") &&
								(doc.get("sede_accessibile_ai_disabili").equals("TOTALMENTE") || doc.get("sede_accessibile_ai_disabili").equals("PARZIALMENTE"))) || // museo
						( doc.containsField("fruibilita_disabili") && doc.get("fruibilita_disabili").equals("1"))  // impianto sportivo
				) {
					if (total > 0) {
						out.write(new String(",").getBytes());
					}
					out.write(doc.toString().getBytes());
					total++;
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

}
