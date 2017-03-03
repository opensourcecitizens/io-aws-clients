package io.parser.utils;

import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class JsonUtils {
	
	public  synchronized Map<String,? extends Object> parseJsonData(byte[] jsondata) throws Exception{
		ObjectMapper mapper = new ObjectMapper();
		Map<String,? extends Object> map =  mapper.readValue(jsondata, new TypeReference<Map<String, ? extends Object>>(){});

		return map;
	}
	
	public synchronized  String objectToJson(Object o) throws Exception{
		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsString(o);

	}
	
	public Object searchJson(String searchKey, String jsonStr) throws Exception{
		Map<String, ?> map = parseJsonData(jsonStr.getBytes());

		return searchMap(searchKey, map);
	}

	public synchronized Object searchMap(String searchkey, Map<String,?>map){

		if(map==null||map.isEmpty())return null;

		Set<String> keyset = map.keySet();
		Object retObj = null;
		for(String key : keyset){
			Object o = map.get(key);
			if(key.equals(searchkey)){ retObj = o;break;}
			else if (o instanceof Map){
				o = searchMap(searchkey,(Map<String,?>)o);
				if(o!=null){retObj = o;break;}
			}else{
				retObj = null;
			}
		}
		return retObj;
	}

	public synchronized Object searchMapFirstSubKey(String searchkey, Map<String,?>map){

		if(map==null||map.isEmpty())return null;

		Set<String> keyset = map.keySet();
		Object retObj = null;
		for(String key : keyset){
			Object o = map.get(key);
			//System.out.println(key+" contains "+searchkey+" ?  "+key.contains(searchkey));
			if(key.contains(searchkey)){ retObj = o;break;}
			else if (o instanceof Map){
				o = searchMapFirstSubKey(searchkey,(Map<String,?>)o);
				if(o!=null){retObj = o;break;}
			}else{
				retObj = null;
			}
		}
		return retObj;
	}

}
