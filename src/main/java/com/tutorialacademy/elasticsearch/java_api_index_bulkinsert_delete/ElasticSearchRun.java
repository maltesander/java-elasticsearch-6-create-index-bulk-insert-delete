package com.tutorialacademy.elasticsearch.java_api_index_bulkinsert_delete;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.parser.ParseException;

public class ElasticSearchRun {
	
	private static final Logger logger = LogManager.getLogger( ElasticSearchRun.class.getName() );
	
	private static String NUMBER_OF_SHARDS  = "number_of_shards";
	private static String NUMBER_OF_REPLICAS= "number_of_replicas";
	
	private static String CLUSTER_NAME 		= "cluster_name";
	
	private static String INDEX_NAME 		= "index_name";
	private static String INDEX_TYPE 		= "index_type";
	
	private static String IP  = "master_ip";
	private static String PORT= "master_port";
	
	public static void main( String[] args ) 
    {
		logger.info( "Starting..." );
		// read properties
		PropertyReader properties = null;
		ElasticSearchConnector es = null;
		
		try {
			properties = new PropertyReader( getRelativeResourcePath( "config.properties" ) );
			
			String numberOfShards  	= properties.read( NUMBER_OF_SHARDS );
			String numberOfReplicas	= properties.read( NUMBER_OF_REPLICAS );
			
			String clusterName 		= properties.read( CLUSTER_NAME );
			
			String indexName 		= properties.read( INDEX_NAME );
			String indexType 		= properties.read( INDEX_TYPE );
			
			String ip 				= properties.read( IP );
			int    port				= Integer.parseInt( properties.read( PORT ) );
		
			es = new ElasticSearchConnector( clusterName, ip, port );
			
			// check if elastic search cluster is healthy
			es.isClusterHealthy();
			
			// check if index already existing
			if( !es.isIndexRegistered( indexName ) ) {
				// create index if not already existing
				es.createIndex( indexName, numberOfShards, numberOfReplicas );
				// manually insert some test data			
				es.bulkInsert( indexName, indexType );
				// insert some test data (from JSON file)
//				es.bulkInsert( indexName, indexType, getRelativeResourcePath( "data.json" ) );
			}
			
			// retrieve elements from the user data where age is in between 15 ad 50
			es.queryResultsWithAgeFilter( indexName, 15, 50 );
			
			es.delete( indexName, "name", "Peter Pan" );

			// retrieve elements from the user data where age is in between 15 ad 50
			es.queryResultsWithAgeFilter( indexName, 15, 50 );
		}
		catch ( FileNotFoundException e ) {
			e.printStackTrace();
		}
		catch ( UnknownHostException e ) {
			e.printStackTrace();
		}
		catch ( IOException e ) {
			e.printStackTrace();
		} 
		// required when parsing JSON
//		catch ( ParseException e ) {
//			e.printStackTrace();
//		}
		finally {
			es.close();
		}
    }
	
	// resolve maven specific path for resources
	private static String getRelativeResourcePath( String resource ) throws FileNotFoundException {
		
		if( resource == null || resource.equals("") ) throw new IllegalArgumentException( resource );
		
		URL url = ElasticSearchRun.class.getClassLoader().getResource( resource );
		
		if( url == null ) throw new FileNotFoundException( resource );
		
		return url.getPath();
	}

}
