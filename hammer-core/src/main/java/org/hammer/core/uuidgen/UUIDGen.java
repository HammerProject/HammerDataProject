package org.hammer.core.uuidgen;

/**
 * 
 * UUID GEN Interface
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Core
 *
 */
public interface UUIDGen {
	/**
	 * UUIDGEN
	 * 
	 * @return
	 */
	String uuidgen();

	/**
	 * UUIDGEN
	 * 
	 * @param nmbr
	 * @return
	 */
	String[] uuidgen(int nmbr);

}
