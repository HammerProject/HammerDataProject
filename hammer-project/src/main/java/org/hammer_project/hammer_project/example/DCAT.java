package org.hammer_project.hammer_project.example;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import org.hammer_project.hammer_project.util.HammerUtils;

import com.mongodb.BasicDBObject;

/**
 * Elaborzione catalogo DACT di dati.dov.it
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project
 *
 */
public class DCAT {

	public static void main(String[] args) throws Exception {
		ArrayList<BasicDBObject> docs = HammerUtils.DCATtoCollection("example/DCAT/dcat.json");
		HashMap<String, BasicDBObject> enti = new HashMap<String, BasicDBObject>();
		HashMap<String, BasicDBObject> regioni = new HashMap<String, BasicDBObject>();
		HashMap<String, BasicDBObject> entiTipi = new HashMap<String, BasicDBObject>();
		HashMap<String, BasicDBObject> localAuth = new HashMap<String, BasicDBObject>();

		for (BasicDBObject doc : docs) {
			BasicDBObject dbj = new BasicDBObject();
			
			BasicDBObject dbj2 = new BasicDBObject();
			
			// "publisher" : { "@type" : "foaf:Agent" , "name" : "Regione
			// Lombardia" , "publisherType" :
			// "http://purl.org/adms/publishertype/RegionalAuthority"}
			String ente = ((BasicDBObject) doc.get("publisher")).get("name").toString().toUpperCase();
			String tipo = "NN";
			if(((BasicDBObject) doc.get("publisher")).keySet().contains("publisherType")) {
				tipo = ((BasicDBObject) doc.get("publisher")).get("publisherType").toString().toUpperCase();
			}
			// dataset__distribution__format

			dbj.put("ente", ente);
			dbj.put("tipo", tipo);
			int num = 1;
			dbj.put("n_dataset", num);

			dbj2.put("n_dataset", num);
			dbj2.put("tipo", tipo);
			
			/** elenco enti **/
			if (!enti.containsKey(ente)) {
				BasicDBObject pos = new BasicDBObject();
				pos = HammerUtils.Google_GeoCoding(ente);
				dbj.append("pos", pos);
				
				enti.put(ente, dbj);
			} else {
				if (enti.get(ente).keySet().contains("n_dataset")) {
					int numOld = enti.get(ente).getInt("n_dataset");
					num = num + numOld;
				}
				enti.get(ente).put("n_dataset", num);
			}
			
			/** elenco tipi enti **/
			if (!entiTipi.containsKey(tipo)) {
				entiTipi.put(tipo, dbj2);
			} else {
				if (entiTipi.get(tipo).keySet().contains("n_dataset")) {
					int numOld = entiTipi.get(tipo).getInt("n_dataset");
					num = numOld+1;
				}
				entiTipi.get(tipo).put("n_dataset", num);
			}
			
			
			/** elenco regioni **/
			if(tipo.equals("HTTP://PURL.ORG/ADMS/PUBLISHERTYPE/REGIONALAUTHORITY")) {
				if (!regioni.containsKey(ente)) {
					regioni.put(ente, dbj);
				} 
			}
			
			
			/** elenco enti locali **/
			if(tipo.equals("HTTP://PURL.ORG/ADMS/PUBLISHERTYPE/LOCALAUTHORITY")) {
				if (!localAuth.containsKey(ente)) {
					localAuth.put(ente, dbj);
				} 
			}

		}

		
		/** elenco completo degli enti **/
		try {
			FileOutputStream out = new FileOutputStream("example/DCAT/enti.json");
			int count = 0;
			int total = 0;
			out.write(new String("[").getBytes());
			for (BasicDBObject doc : enti.values()) {

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
			System.out.println("Totale enti scritti " + total);
			System.out.println("Totale enti letti " + docs.size());
			System.out.println("Totale enti elaborati " + count);

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		/** elenco regioni **/
		try {
			FileOutputStream out = new FileOutputStream("example/DCAT/dataset_regioni.json");
			int count = 0;
			int total = 0;
			out.write(new String("[").getBytes());
			for (BasicDBObject doc : regioni.values()) {

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
			System.out.println("Totale regioni " + total);
			System.out.println("Totale regioni elaborate " + count);

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		/** elenco tipi enti **/
		try {
			FileOutputStream out = new FileOutputStream("example/DCAT/tipi_enti.json");
			int count = 0;
			int total = 0;
			out.write(new String("[").getBytes());
			for (BasicDBObject doc : entiTipi.values()) {

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
			System.out.println("Totale tipi " + total);
			System.out.println("Totale tipi elaborate " + count);

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		/** elenco enti locali **/
		try {
			FileOutputStream out = new FileOutputStream("example/DCAT/local.json");
			int count = 0;
			int total = 0;
			out.write(new String("[").getBytes());
			for (BasicDBObject doc : localAuth.values()) {

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
			System.out.println("Totale local auth " + total);
			System.out.println("Totale local auth elaborate " + count);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	
	
	

}
