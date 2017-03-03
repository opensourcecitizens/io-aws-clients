package com.neustar.io.net.forward.rest.json;

import java.io.Serializable;
import java.util.Map;



public interface ForwarderIfc<V> extends Serializable{
	
	
	public String forward( Map<String,?>map, V schema) throws Throwable; 
	public String forward( Map<String,?>map, V schema, Map<String,?> attr) throws Throwable; 
	
	//public String forward( Object data, Schema schema) throws Throwable; 
	//public String forward( Object data, Schema schema, Map<String,?> attr) throws Throwable; 
	
}
