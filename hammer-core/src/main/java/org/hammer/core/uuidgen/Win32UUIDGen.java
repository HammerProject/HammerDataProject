package org.hammer.core.uuidgen;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * WIn 32 UUID Gen
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Core
 *
 */
public class Win32UUIDGen implements UUIDGen {

	/**
	 * 
	 * Constructor
	 * 
	 */
	public Win32UUIDGen() {
		LOG.info("Creates a new instance of Win32UUIDGen");
	}

	public String uuidgen() {
		final String[] uuids = this.uuidgen(1);
		return uuids[0];
	}

	public String[] uuidgen(final int nmbr) {
		String[] uuids = new String[nmbr];
		try {
			final Runtime runTime = Runtime.getRuntime();
			final Process process = runTime.exec("uuidgen -n" + nmbr);
			final BufferedReader buffer = new BufferedReader(new InputStreamReader(process.getInputStream()));
			for (int i = 0; i < nmbr; ++i) {
				uuids[i] = buffer.readLine();
			}
		} catch (IOException ex) {
			LOG.error("ERROR EIN WIN32UUIDGEN", ex);
		}

		return uuids;
	}

	/**
	 * Log
	 */
	private static final Log LOG = LogFactory.getLog(Win32UUIDGen.class);

}
