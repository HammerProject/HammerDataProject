package org.hammer.taino;

import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;




/**
 * Taino
 *
 */
public class Taino extends HttpServlet {

	/**
	 * Connessione al database
	 */
	private static final String DB_ADD = "mongodb://hammerdb-instance-1:27017/hammer.";
	//private static final String DB_ADD = "mongodb://192.168.56.90:27017/hammer.";
	

	/**
	* 
	*/
	private static final long serialVersionUID = 4770375639662655227L;

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		response.setContentType("application/json");
    	MongoClient mongo = null;
		final HashMap<String, Document> dataMap = new HashMap<String, Document>();
        MongoDatabase db = null;
        System.out.println("Taino gets data set from database...");
        try {
        	String input = DB_ADD + request.getParameter("table");
        	MongoClientURI inputURI = new MongoClientURI(input);
			mongo = new MongoClient(inputURI);
			db =  mongo.getDatabase(inputURI.getDatabase());
			
			MongoCollection<Document> result =  db.getCollection(inputURI.getCollection());
			
			BasicDBObject searchQuery = new BasicDBObject(new BasicDBObject());
			
			FindIterable<Document> iterable = result.find(searchQuery);
            
			System.out.println("Colombo gets data set from database..." + searchQuery.toString());
            
			
			
            iterable.forEach(new Block<Document>() {
            	
                public void apply(final Document document) {
                		dataMap.put(document.getString("_id"), document);
                	
                }
            });
            
            
            response.getWriter().write("[");
			int count = 0;
			for(Document d : dataMap.values()) {
				response.getWriter().write(d.toJson());
				count ++;
				if(count != dataMap.size()) {
					response.getWriter().write(",");
				}
			}
			response.getWriter().write("]");
			response.flushBuffer();
			
        } catch (Exception ex) {
            throw ex;
        }finally {
			if(mongo!=null) { mongo.close();}
		}

	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		this.doPost(request, response);
	}

}
