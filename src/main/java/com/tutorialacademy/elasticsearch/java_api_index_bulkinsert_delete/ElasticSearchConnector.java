package com.tutorialacademy.elasticsearch.java_api_index_bulkinsert_delete;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ElasticSearchConnector {
	private static final Logger logger = LogManager.getLogger( ElasticSearchConnector.class.getName() );
	
	private TransportClient client = null;
	
	public ElasticSearchConnector( String clusterName, String clusterIp, int clusterPort ) throws UnknownHostException {
		
		Settings settings = Settings.builder()
				  .put( "cluster.name", clusterName )
				  .put( "client.transport.ignore_cluster_name", true )
				  .put( "client.transport.sniff", true )
				  .build();
				
				// create connection
				client = new PreBuiltTransportClient( settings ); 
				client.addTransportAddress( new TransportAddress( InetAddress.getByName( clusterIp ), clusterPort) );
				
		logger.info( "Connection " + clusterName + "@" + clusterIp + ":" + clusterPort + " established!" );		
	}
	
	public boolean isClusterHealthy() {

		final ClusterHealthResponse response = client
			    .admin()
			    .cluster()
			    .prepareHealth()
			    .setWaitForGreenStatus()
			    .setTimeout( TimeValue.timeValueSeconds( 2 ) )
			    .execute()
			    .actionGet();

		if ( response.isTimedOut() ) {
			logger.info( "The cluster is unhealthy: " + response.getStatus() );
			return false;
		}

		logger.info( "The cluster is healthy: " + response.getStatus() );
		return true;
	}
	
	public boolean isIndexRegistered( String indexName ) {
		// check if index already exists
		final IndicesExistsResponse ieResponse = client
			    .admin()
			    .indices()
			    .prepareExists( indexName )
			    .get( TimeValue.timeValueSeconds( 1 ) );
			
		// index not there
		if ( !ieResponse.isExists() ) {
			return false;
		}
		
		logger.info( "Index already created!" );
		return true;
	}
	
	public boolean createIndex( String indexName, String numberOfShards, String numberOfReplicas ) {
		CreateIndexResponse createIndexResponse = 
			client.admin().indices().prepareCreate( indexName )
        	.setSettings( Settings.builder()             
                .put("index.number_of_shards", numberOfShards ) 
                .put("index.number_of_replicas", numberOfReplicas )
        	)
        	.get(); 
				
		if( createIndexResponse.isAcknowledged() ) {
			logger.info("Created Index with " + numberOfShards + " Shard(s) and " + numberOfReplicas + " Replica(s)!");
			return true;
		}
		
		return false;				
	}
	
	public boolean bulkInsert( String indexName, String indexType ) throws IOException { 
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		
		// either use client#prepare, or use Requests# to directly build index/delete requests
		bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add( 
			client.prepareIndex( indexName, indexType, null )
		        .setSource( XContentFactory.jsonBuilder()
	                .startObject()
	                    .field( "name", "Mark Twain" )
	                    .field( "age", 75 )
	                .endObject()
	    ));

		bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add( 
			client.prepareIndex( indexName, indexType, null )
		        .setSource( XContentFactory.jsonBuilder()
	                .startObject()
	                    .field( "name", "Tom Saywer" )
	                    .field( "age", 12 )
	                .endObject()
		));
		
		bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add( 
			client.prepareIndex( indexName, indexType, null )
		        .setSource( XContentFactory.jsonBuilder()
	                .startObject()
	                    .field( "name", "John Doe" )
	                    .field( "age", 20 )
	                .endObject()
		));
		
		bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add( 
			client.prepareIndex( indexName, indexType, null )
		        .setSource( XContentFactory.jsonBuilder()
	                .startObject()
	                    .field( "name", "Peter Pan" )
	                    .field( "age", 15 )
	                .endObject()
		));
		
		bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add( 
			client.prepareIndex( indexName, indexType, null )
		        .setSource( XContentFactory.jsonBuilder()
	                .startObject()
	                    .field( "name", "Johnnie Walker" )
	                    .field( "age", 37 )
	                .endObject()
		));

		BulkResponse bulkResponse = bulkRequest.get();
		if ( bulkResponse.hasFailures() ) {
		    // process failures by iterating through each bulk response item
			logger.info( "Bulk insert failed!" );
			return false;
		}
		
		return true;
	}
	
	/**
	 * Bulk insert documents from a JSON array from file
	 * @param indexName name of the index 
	 * @param indexType type of the index
	 * @param dataPath path to the JSON data file 
	 * @return true if insert was successful, else false
	 * @throws IOException
	 * @throws ParseException
	 */
	public boolean bulkInsert( String indexName, String indexType, String dataPath ) throws IOException, ParseException { 
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		
		JSONParser parser = new JSONParser();
		// we know we get an array from the example data
		JSONArray jsonArray = (JSONArray) parser.parse( new FileReader( dataPath ) );
	    
		@SuppressWarnings("unchecked")
		Iterator<JSONObject> it = jsonArray.iterator();
	    
	    while( it.hasNext() ) {
	    	JSONObject json = it.next();
	    	logger.info( "Insert document: " + json.toJSONString() );	
	    	
			bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add( 
				client.prepareIndex( indexName, indexType )
					.setSource( json.toJSONString(), XContentType.JSON )
			);
	    }
        
		BulkResponse bulkResponse = bulkRequest.get();
		if ( bulkResponse.hasFailures() ) {
			logger.info( "Bulk insert failed: " + bulkResponse.buildFailureMessage() );
			return false;
		}
		
		return true;
	}
	
	/**
	 * Predefined template to query our name / age user data
	 * @param indexName index name of where to execute the search query
	 * @param from age range min
	 * @param to age range max
	 */
	public void queryResultsWithAgeFilter( String indexName, int from, int to ) {
		SearchResponse scrollResp = 
			client.prepareSearch( indexName )
			// sort order
	        .addSort( FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC )
	        // keep results for 60 seconds
	        .setScroll( new TimeValue( 60000 ) )
	        // filter for age
	        .setPostFilter( QueryBuilders.rangeQuery( "age" ).from( from ).to( to ) )
	        // maximum of 100 hits will be returned for each scroll
	        .setSize( 100 ).get(); 
		
		// scroll until no hits are returned
		do {
			int count = 1;
		    for ( SearchHit hit : scrollResp.getHits().getHits() ) {
		    	Map<String,Object> res = hit.getSourceAsMap();
		    	
		    	// print results
		    	for( Map.Entry<String,Object> entry : res.entrySet() ) {
		    		logger.info( "[" + count + "] " + entry.getKey() + " --> " + entry.getValue() );
		    	}
		    	count++;
		    }

		    scrollResp = client.prepareSearchScroll( scrollResp.getScrollId() ).setScroll( new TimeValue(60000) ).execute().actionGet();
		// zero hits mark the end of the scroll and the while loop.
		} while( scrollResp.getHits().getHits().length != 0 ); 
	}
	
	/**
	 * Delete a document identified by a key value pair
	 * @param indexName name of the index where to delete
	 * @param key pair key
	 * @param value pair value
	 * @return number of deleted documents
	 */
	public long delete( String indexName, String key, String value ) {
		BulkByScrollResponse response =
			    DeleteByQueryAction.INSTANCE.newRequestBuilder( client )
			        .filter( QueryBuilders.matchQuery( key, value ) ) 
			        .source( indexName )
			        .refresh( true )
			        .get();                                             

		logger.info( "Deleted " + response.getDeleted() + " element(s)!" );
		
		return response.getDeleted();
	}
	
	/**
	 * Close the ES client properly
	 */
	public void close() {
		if( client != null ) client.close();
	}
	
}
