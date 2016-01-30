package org.hammer.core.uuidgen;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hammer.core.exception.UUIDGenBadConfigurationException;

/**
 * 
 * Factory UUID Gen
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Core
 *
 */
public class UUIDGenFactory {

	/**
	 * 
	 * Constructor
	 * 
	 */
	private UUIDGenFactory() {
	}

	/**
	 * Log
	 */
	private static final Log LOG = LogFactory.getLog(UUIDGenFactory.class);

	/**
	 * Impl.
	 */
	private static UUIDGen uuidgen = null;

	public static synchronized UUIDGen getDefaultUUIDGen() throws UUIDGenBadConfigurationException {
		if (uuidgen == null) {
			uuidgen = createDefaultUUIDGen();
		}
		if (uuidgen == null) {
			throw new UUIDGenBadConfigurationException();
		}
		return uuidgen;
	}

	private static synchronized UUIDGen createDefaultUUIDGen() {
		if (uuidgen == null) {
			LOG.info("Implementazione dell'interfaccia UUIDGen: " + DefaultUUIDGen.class);
						try {
				uuidgen = new DefaultUUIDGen();
			} catch (Exception e) {
				LOG.error("HAMMER : Si è verificato un errore durante l'istanziazione della classe " + DefaultUUIDGen.class + ", implementazione dell'interfaccia UUIDGen.");
				LOG.error(e);
			}
		}
		return uuidgen;
	}

}
