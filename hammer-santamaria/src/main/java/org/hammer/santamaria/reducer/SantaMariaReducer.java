package org.hammer.santamaria.reducer;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.mongodb.hadoop.io.BSONWritable;


/**
 * Reduces
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public class SantaMariaReducer extends Reducer<Text, BSONWritable, Text, BSONWritable> {
	
	public static final Log LOG = LogFactory.getLog(SantaMariaReducer.class);
	
	
	@Override
    public void reduce( final Text pKey, final Iterable<BSONWritable> pValues, final Context pContext )
            throws IOException, InterruptedException{
        
		LOG.debug("START SANTA MARIA REDUCER");
		int count = 0;
        for ( final BSONWritable value : pValues ){
        	value.getDoc().put("last-find", (new Date()));
        	pContext.write( pKey, value);        	
            count++;
        }

        LOG.debug("FOUND AND WRITE " + count + " DATASET ");
        

    }   
}