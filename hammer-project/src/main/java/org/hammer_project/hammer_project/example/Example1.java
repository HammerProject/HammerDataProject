package org.hammer_project.hammer_project.example;

import java.io.FileOutputStream;
import java.util.ArrayList;

import org.hammer_project.hammer_project.util.HammerUtils;

import com.mongodb.BasicDBObject;

/**
 * Example 1
 * Polluted and sports site in Bergamo
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project
 *
 */
public class Example1 {

	public static void main(String[] args) throws Exception {
		ArrayList<BasicDBObject> docs = HammerUtils.GetFromTaino("result_177932738");
		int c = docs.size();
		for(BasicDBObject doc: docs) {
			String indirizzo = "";
			if(doc.keySet().contains("indirizzo")) {
				indirizzo += doc.getString("indirizzo");
			}
			if(doc.keySet().contains("indirizzo_localita")) {
				indirizzo += doc.getString("indirizzo_localita") + ", ";
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
			} else {
				pos = HammerUtils.Google_GeoCoding(indirizzo.trim());
				if(pos == null || !pos.keySet().contains("lat")) {
					pos = HammerUtils.Google_GeoCoding(comune.trim());
				}
			}
			
			doc.append("indirizzo_new", indirizzo);
			doc.append("pos", pos);
			//Thread.sleep(1000);
			c--;
			System.out.println(c);
		}
		try {
			FileOutputStream out = new FileOutputStream("query_1.json");
			int count = 0;
			out.write(new String("[").getBytes());
			for(BasicDBObject doc : docs) {
				out.write(doc.toString().getBytes());
				count ++;
				if(count != docs.size()) {
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
