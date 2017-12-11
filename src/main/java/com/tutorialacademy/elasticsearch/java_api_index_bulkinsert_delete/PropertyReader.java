package com.tutorialacademy.elasticsearch.java_api_index_bulkinsert_delete;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class PropertyReader {

	private String filePath = null;
	private Properties properties = null;
	
	/**
	 * Read and parse the properties file and store in class object
	 */
	public PropertyReader( String filePath ) {
		this.filePath = filePath;
		
    	properties = new Properties();
		// open config file
    	InputStream in = null;
		try {
			in = new FileInputStream( filePath );
			properties.load( in );
		} catch ( Exception e ) {
			e.printStackTrace();
		}
		finally {
    		if ( in != null ) {
    			try {
    				in.close();
    			} catch ( IOException e ) {
    				e.printStackTrace();
    			}
    		}
		}
	}
	
	/**
	 * Read from the in memory class object; no file access required
	 */
	public String read( String key ) {
		return properties.getProperty( key );
	}

	/**
	 * Write to the class object (changes available at runtime) and persist the data onto the file system
	 */
	public void write( String key, String value ) {
		properties.setProperty( key, value );
		
		// persist data into property file
		OutputStream output = null;

    	try {
    		output = new FileOutputStream( filePath );

    		// set the properties key-value pair
    		properties.setProperty( key, value );
    		// save properties to given path
    		properties.store( output, null );

    	} catch ( IOException ioe ) {
    		ioe.printStackTrace();
    	} finally {
    		if ( output != null ) {
    			try {
    				output.close();
    			} catch ( IOException e ) {
    				e.printStackTrace();
    			}
    		}

    	}
	}

}
