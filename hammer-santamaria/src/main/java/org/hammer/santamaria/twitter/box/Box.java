package org.hammer.santamaria.twitter.box;

/**
 * Standard Box
 *
 *
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Santa Maria
 *
 */
public class Box {

	public static final Box LOMBARDIA = new Box(new double[][]{
		 {8.664600d, 44.816259d },
		 {10.935321d, 46.529647d }   
	});
	
	public double[][] MYBOX = {
			 {8.664600d, 44.816259d },
			 {10.935321d, 46.529647d }
           
	};

	/**
	 * Constructor for my box
	 * @param myBox a box: SW(ltd, lat) + NE(ltd, lat)
	 */
	public Box(double[][] myBox) {
		super();
		this.MYBOX = myBox;
	}
	
}
