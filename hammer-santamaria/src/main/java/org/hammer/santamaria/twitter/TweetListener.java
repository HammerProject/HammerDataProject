package org.hammer.santamaria.twitter;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.util.JSON;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;

/**
 * 
 * Listerner for Twitter Streams
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Santa Maria
 *
 */
public class TweetListener implements StatusListener {
	
	private int updated = 0;
	private int inserted = 0;
	
	/**
	 * LOG 
	 */
	private static final Log LOG = LogFactory.getLog(TweetListener.class);
	
	private final MongoCollection<Document> collection;
	
	public TweetListener(final MongoCollection<Document> collection) {
		this.collection = collection;
		inserted = 0;
		updated = 0;
	}
	
	public void onException(Exception exp) {
		exp.printStackTrace();
	}

	public void onDeletionNotice(StatusDeletionNotice arg0) {
	}

	public void onScrubGeo(long arg0, long arg1) {
	}

	public void onStallWarning(StallWarning arg0) {
	}

	public void onStatus(Status status) {
		LOG.info("@" + status.getUser().getScreenName() + " - " + status.getText());
		LOG.info("From --> " + status.getUser().getLocation());
		LOG.info("--------------------------------------------------------");
		handleStatus(status);
	}

	public void onTrackLimitationNotice(int arg0) {
	}

	protected void handleStatus(Status tweet) {
		if (tweet.isRetweet()) {
			return;
		}
		LOG.info("--> Save to DB");
		try {
			String json = TwitterObjectFactory.getRawJSON(tweet);
			BasicDBObject bo = (BasicDBObject) JSON.parse(json);
			BasicDBObject searchQuery = new BasicDBObject().append("_id", bo.get("_id"));
			FindIterable<Document> c = collection.find(searchQuery);
			if (c.first() != null) {
				collection.replaceOne(bo, new Document(bo));
				updated++;
			} else {
				collection.insertOne(new Document(bo));
				inserted++;
			}
			LOG.info("--> inserted " + inserted);
			LOG.info("--> updated " + updated);

		} catch (Exception e) {
			LOG.error(e);
			LOG.error("SANTA MARIA TWITTER STREAM: Error", e);

		}

	}

	


}
