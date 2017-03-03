package com.neustar.io.net.forward.rest.json;

import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.schema.JsonSchema;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;


public class ElasticSearchPostForwarder implements ForwarderIfc<JsonSchema>{

	private static final long serialVersionUID = 1L;
	
	private String uri = null;
	private static ElasticSearchPostForwarder singleton = null;
	
	private ElasticSearchPostForwarder(){
	}
	
	private ElasticSearchPostForwarder(String _uri){
		uri= _uri;
	}
	
	public static ForwarderIfc singleton(String _uri) {
		
		if(singleton==null){
			singleton=new ElasticSearchPostForwarder();
		}
		
		singleton.setUri(_uri);
		return singleton;
	}
	
	public static ForwarderIfc instance(String _uri) {
		return new ElasticSearchPostForwarder(_uri);
	}

	
	private WebResource webResource = null;

	public WebResource getWebResource(){
		if(webResource==null){
			ClientConfig config = new DefaultClientConfig();
			Client client = Client.create(config);
			webResource = client.resource(UriBuilder.fromUri(uri).build());
		}
		return webResource;
	}
	
	@Override
	public synchronized String forward(Map<String, ?> map, JsonSchema schema) throws Throwable {
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(map);
		
		webResource = getWebResource();
		Builder builder = webResource.accept(MediaType.APPLICATION_JSON);
		builder.type(MediaType.APPLICATION_JSON );
		
		ClientResponse cliResponse = builder.post(ClientResponse.class, json);
		
		return cliResponse.getEntity(String.class);
	}
	
	@Override
	public String forward(Map<String, ?> map, JsonSchema schema, Map<String, ?> attr) throws Throwable {

		return forward(map,schema);
	}
	
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}

}

