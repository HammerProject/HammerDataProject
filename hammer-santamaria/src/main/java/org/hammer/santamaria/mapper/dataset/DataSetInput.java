package org.hammer.santamaria.mapper.dataset;

import org.bson.BSONObject;

/**
 * Contratto per gli oggetti Data Set Input
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Santa Maria
 *
 */
public interface DataSetInput {

	public BSONObject getDataSet(String url, String datasource, String id, BSONObject c);
	
}
