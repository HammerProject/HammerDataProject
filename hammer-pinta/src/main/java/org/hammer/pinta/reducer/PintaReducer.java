package org.hammer.pinta.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;


/**
 * Reducer
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project - Pinta
 *
 */
public class PintaReducer extends Reducer<Text, BSONWritable, Text, BSONWritable> {
	
	public static final Log LOG = LogFactory.getLog(PintaReducer.class);
	
	
	@Override
    public void reduce( final Text pKey, final Iterable<BSONWritable> pValues, final Context pContext )
            throws IOException, InterruptedException{
        
		LOG.debug("START PINTA REDUCER");
		ArrayList<BSONObject> documents = new ArrayList<BSONObject>();
        for ( final BSONWritable value : pValues ){
        	documents.add(value.getDoc());
        }
        //(Configuration conf = pContext.getConfiguration();
        
        String keyword = pKey.toString();
		BasicDBObject obj = new BasicDBObject("keyword", keyword);
		obj.append("documents", documents);
		obj.append("last-update", (new Date()));
		pContext.write( pKey, new BSONWritable(obj));
		
		/*ArrayList<String> synonyms = ThesaurusUtils.Get(conf.get("thesaurus.url"), keyword,conf.get("thesaurus.lang"), conf.get("thesaurus.key"), "json");
		for (String synonym : synonyms) {
			BasicDBObject temp = new BasicDBObject("keyword", synonym);
			temp.append("document", document);
			temp.append("last-update", (new Date()));			
			pContext.write( new Text(synonym), new BSONWritable(temp));
			
		}*/
		
        
        LOG.debug("PINTA - FOUND AND WRITE " + pKey + " DATASET ");
        

    }   
}