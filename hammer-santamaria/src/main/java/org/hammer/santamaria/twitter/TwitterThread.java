package org.hammer.santamaria.twitter;

import java.net.URISyntaxException;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * 
 * Twitter Thread for download tweet stream from a GeoBox
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Santa Maria
 *
 */
public class TwitterThread implements Runnable {

	private String MY_COLLECTION = "mongodb://192.168.56.90:27017/hammer.tweets";
	
	private static final Log LOG = LogFactory.getLog(TwitterThread.class);

	private double[][] box = { { 9.689785d, 45.665891d }, { 9.735845d, 45.667017d } };

	private TwitterStream twitterStream = null;
	
	private TweetListener myListerner = null;
	
	/**
	 * Costructor
	 * 
	 * @param myBox
	 * @throws Exception
	 */
	public TwitterThread(String myBox) throws Exception {
		try {
			PropertiesConfiguration config = new PropertiesConfiguration("hammer.properties");
			config.setProperty("colors.background", "#000000");
			config.save();
			
			LOG.info("OAuthConsumerKey = " + config.getProperty("twitter.oauthconsumerkey"));
			LOG.info("OAuthConsumerSecret = " + config.getProperty("twitter.oauthconsumersecret"));
			LOG.info("OAuthAccessToken = " + config.getProperty("twitter.oauthaccesstoken"));
			LOG.info("OAuthAccessTokenSecret = " + config.getProperty("twitter.oauthaccesstokensecret"));
			LOG.info("Collection = " + config.getProperty("twitter.collection"));
			
			MY_COLLECTION = config.getProperty("twitter.collection").toString();
			
			ConfigurationBuilder cb = new ConfigurationBuilder();
			cb.setDebugEnabled(true).setOAuthConsumerKey(config.getProperty("twitter.oauthconsumerkey").toString())
					.setOAuthConsumerSecret(config.getProperty("twitter.oauthconsumersecret").toString())
					.setOAuthAccessToken(config.getProperty("twitter.oauthaccesstoken").toString())
					.setOAuthAccessTokenSecret(config.getProperty("twitter.oauthaccesstokensecret").toString());
			cb.setJSONStoreEnabled(true);
			TwitterStreamFactory tf = new TwitterStreamFactory(cb.build());

			this.twitterStream = tf.getInstance();
			switch(myBox) {
				case "lombardia": this.box = org.hammer.santamaria.twitter.box.Box.LOMBARDIA.MYBOX; break;
				default : throw new Exception("myBox " + myBox + " unknow!!!"); 
			}
			

			init();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	public void run() {
		
		twitterStream.addListener(myListerner);
		FilterQuery filter = new FilterQuery();
		filter.locations(box);
		twitterStream.filter(filter);
	}

	public void init() throws TwitterException, URISyntaxException {
		MongoClient mongo = null;
		MongoDatabase db = null;
		LOG.info("Twitter Thread connect to db...");
		try {
			MongoClientURI inputURI = new MongoClientURI(MY_COLLECTION);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());
			if (db.getCollection("tweets") == null) {
				db.createCollection("tweets");
			}
			MongoCollection<Document> tweets = db.getCollection("tweets");
			myListerner = new TweetListener(tweets);
		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
		} finally {
			if (mongo != null) {
				//mongo.close();
			}
		}

	}

}
