package org.hammer_project.hammer_project;

import java.io.UnsupportedEncodingException;

import org.hammer_project.hammer_project.util.HammerUtils;

/**
 * Main
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hamme Project - Main" );
        try {
			HammerUtils.Google_GeoCoding("ONETA");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
