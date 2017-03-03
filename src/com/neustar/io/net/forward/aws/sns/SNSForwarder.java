package com.neustar.io.net.forward.aws.sns;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.neustar.io.net.forward.rest.json.ForwarderIfc;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;

import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.schema.JsonSchema;

import com.sun.jersey.api.client.WebResource.Builder;

public class SNSForwarder implements ForwarderIfc<JsonSchema> {

	Logger log = Logger.getLogger(SNSForwarder.class);
	private static final long serialVersionUID = 1L;

	String hdfsuri = null;
	private SNSForwarder(String uri) throws SQLException, ClassNotFoundException {
			hdfsuri = uri;// = _zookeeper_quorum;
	}
	
	private SNSForwarder(){}
	
	private static SNSForwarder forwarderInstance = null;
	
	public static SNSForwarder singleton(String zookeeper_quorum) throws ClassNotFoundException, SQLException{
		
		if(forwarderInstance==null){
			forwarderInstance = new SNSForwarder(zookeeper_quorum);
		}
		
		return forwarderInstance;
	} 

	
	
	@Override
	public void finalize() throws Throwable{		
		super.finalize();
	}

	@Override
	public synchronized String forward(Map<String, ? extends Object> map, JsonSchema schema) throws Throwable {

		String ret = "SUCCESS";
		try{
			
			ObjectMapper mapper = new ObjectMapper();
			String json = mapper.writeValueAsString(map);
			
			//appendToHDFS(hdfsuri,json);
			
			
		}catch(Exception e){
			log.error(e,e);
			ret = "ERROR due to"+e.getMessage();
		}
		
		log.info("appending to '"+hdfsuri+"' returned "+ret);
		
		return  ret;
	}
	
	@Override
	public String forward(Map<String, ? extends Object> map, JsonSchema schema, Map<String, ?> attr) throws Throwable {
		return forward(map,schema);
	}
}


