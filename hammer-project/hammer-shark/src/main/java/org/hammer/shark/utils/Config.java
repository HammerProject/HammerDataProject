package org.hammer.shark.utils;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkFiles;

/**
 * Set and get properties from file
 * 
 *
 */
public class Config {
	/**
	 * Log
	 */
	private static final Log LOG = LogFactory.getLog(Config.class);
	
	/**
	 * Properties
	 */
	private PropertiesConfiguration config;

	/**
	 * The property file name
	 */
	public static String PROPERTY_FILE_NAME = "my.conf";
	
	/**
	 * Build a config object
	 */
	private Config() {
		try {
			LOG.info("Load configuration file from: " + SparkFiles.get(Config.PROPERTY_FILE_NAME));
			config = new PropertiesConfiguration(SparkFiles.get(Config.PROPERTY_FILE_NAME));
			config.load();
		} catch (Exception e1) {
			LOG.error("ERROR IN CONFIG ", e1);
			
		}
	}

	/**
	 * Holder for Config
	 * @author  Mauro Pelucchi
	 * @email   mauro.pelucchi@gmail.com
	 * @project JobMe
	 *
	 */
	private static class ConfigHolder {
		private final static Config instance = new Config();
	}
	
	/**
	 * Init the configuration object
	 * 
	 * @param configFile
	 */
	public static Config init(String configFile) {
		PROPERTY_FILE_NAME = configFile;
		return ConfigHolder.instance;
	}

	/**
	 * Get the configuration instance (before to call getInstance make sure to init)
	 * 
	 * @return a Config Object
	 */
	public static Config getInstance() {
		return ConfigHolder.instance;
	}


	/**
	 * Get configuration
	 * @return
	 */
	public PropertiesConfiguration getConfig() {
		return config;
	}

	/**
	 * Set the configuration
	 * 
	 * @param config
	 */
	public void setConfig(PropertiesConfiguration config) {
		this.config = config;
	}
	
	

}
