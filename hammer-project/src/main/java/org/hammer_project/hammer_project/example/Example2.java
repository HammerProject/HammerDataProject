package org.hammer_project.hammer_project.example;

import java.io.FileOutputStream;
import java.util.ArrayList;

import org.hammer_project.hammer_project.util.HammerUtils;

import com.mongodb.BasicDBObject;

/**
 * Example 1
 * School in Italy
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project
 *
 */
public class Example2 {

	public static void main(String[] args) throws Exception {
		ArrayList<BasicDBObject> docs = HammerUtils.GetFromTaino("result93896758");
		ArrayList<BasicDBObject> list = new ArrayList<BasicDBObject>();
		int total = docs.size();
		int c = docs.size();
		int ok = 0;
		for(BasicDBObject doc: docs) {
			c--;
			System.out.println(c);
			String descrizione = "";
			String type = "";
			if(doc.keySet().contains("denominazione_scuola_infanzia")) {
				descrizione = doc.getString("denominazione_scuola_infanzia");
				type = "scuola_infanzia";
				ok++;
			} else if(doc.keySet().contains("TIPO_SCUOLA")) {
				descrizione = doc.getString("TIPO_SCUOLA") + " " + doc.getString("UBICAZIONE");
				type = doc.getString("TIPO_SCUOLA");
				ok++;
			} else if(doc.keySet().contains("ragione_sociale")) {
				descrizione = doc.getString("ragione_sociale");
				type = "fattoria_didattica";
				ok++;
			} else if(doc.keySet().contains("denominazione") && doc.keySet().contains("tipologia")) {
				descrizione = doc.getString("denominazione");
				type = doc.getString("tipologia");
				ok++;
			} else if(doc.keySet().contains("denom_sede") && doc.keySet().contains("macrotipologia_sede")) {
				descrizione = doc.getString("ragione_sociale");
				type = doc.getString("macrotipologia_sede");
				ok++;
			} else {
				continue;
			}
			
			
			String indirizzo = "";
			if(doc.keySet().contains("indirizzo")) {
				indirizzo += doc.getString("indirizzo") + ", ";
			}
			if(doc.keySet().contains("indirizzo_localita")) {
				indirizzo += doc.getString("indirizzo_localita");
			}
			String comune = "";
			if(doc.keySet().contains("comune")) {
				indirizzo += doc.getString("comune");
				comune = doc.getString("comune");
			}
			BasicDBObject pos = new BasicDBObject();
			if(doc.keySet().contains("location")) {
				pos.append("lat", ((BasicDBObject) doc.get("location")).get("latitude"));
				pos.append("lon", ((BasicDBObject) doc.get("location")).get("longitude"));
			} else if(doc.keySet().contains("lng")) {
				pos.append("lat", doc.get("lat"));
				pos.append("lon", doc.get("lng"));
			} else if(doc.keySet().contains("LON")) {
				pos.append("lat", doc.get("LAT"));
				pos.append("lon", doc.get("LON"));
			} else {
				pos = HammerUtils.Google_GeoCoding(indirizzo.trim());
				if(pos == null || !pos.keySet().contains("lat")) {
					pos = HammerUtils.Google_GeoCoding(comune.trim());
				}
			}
			
			doc.append("indirizzo_new", indirizzo);
			doc.append("pos", pos);
			doc.append("descrizione_scuola", descrizione);
			doc.append("tipo_scuola", type);
			list.add(doc);
		}
		System.out.println("Total --> " + total);
		System.out.println("Ok --> " + ok);
		System.out.println("Check --> " + list.size());
		try {
			FileOutputStream out = new FileOutputStream("query_2.json");
			int count = 0;
			out.write(new String("[").getBytes());
			for(BasicDBObject doc : list) {
				out.write(doc.toString().getBytes());
				count ++;
				if(count != list.size()) {
					out.write(new String(",").getBytes());
				}
			}
			out.write(new String("]").getBytes());
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
