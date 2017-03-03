package com.neustar.aws.lambda;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.neustar.io.net.forward.rest.json.ForwarderIfc;
import com.neustar.io.net.forward.rest.json.ElasticSearchPostForwarder;

import io.parser.utils.JsonUtils;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

public class StreamsMapToESIndices implements RequestHandler<KinesisEvent, String>{

	private String indexUrlStr =null;
	private String guidKey = null;
	//private Properties properties = null;
	{
		
		indexUrlStr = System.getenv("indexUrl");
		guidKey = System.getenv("guidKey");
		
	}
	
	private JsonUtils utils = new JsonUtils();

	/**
		// Put Record Batch records. Max No.Of Records we can put in a
		// single put record batch request is 500
		// firehoseClient.putRecordBatch(putRecordBatchRequest);
	 **/
	@Override
	public String handleRequest(KinesisEvent event, Context context) {

		LambdaLogger logger = context.getLogger();

		List<KinesisEventRecord> recordList = event.getRecords();

		Object guidValue = null;
		
		for(KinesisEventRecord rec:recordList){

			ByteBuffer data = rec.getKinesis().getData();
			
			
			try{
				
				@SuppressWarnings("unchecked")
				Map<String, Object> mappedData  = (Map<String, Object>) utils.parseJsonData(data.array());
				guidValue = utils.searchMap(guidKey, mappedData);
				
				logger.log(guidKey+" = "+guidValue);
				
				return saveToES(mappedData, guidValue.toString(), logger);
				
				
			} catch (Throwable e) {
				
				logger.log("Error Message: " + e.getMessage());
				
				//write to error kinesis stream
				
				return "Error due to "+e.getMessage();
			}

		}

		recordList.clear();

		return "Success";
	}
	
	private String saveToES(Map<String, Object> map, String indexKey,
	        LambdaLogger logger) throws Throwable {
	    
	        logger.log("Writing message to index with key "+indexKey);

			map.put("createdate",  DateFormat.getDateInstance().format(new Date()));
			map.put("messageid", UUID.randomUUID()+"");
						
			ForwarderIfc<?> restForward = ElasticSearchPostForwarder.instance(indexUrlStr+"_"+indexKey+"/events");	
			String response = restForward.forward(map,null);

			logger.log("ES return response : "+response);
			System.out.println("ES return response : "+response);
			return response;
			
	}


}
