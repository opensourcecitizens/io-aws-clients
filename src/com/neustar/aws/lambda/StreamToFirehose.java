package com.neustar.aws.lambda;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;




public class StreamToFirehose implements RequestHandler<KinesisEvent, String>{

	private String firehoseDeliveryStreamName =null;
	private String firehoseEndpointUrl = null;

	private  static final Logger logger = Logger.getLogger(StreamToFirehose.class);
	 
	private AmazonKinesisFirehoseClient firehoseClient = null;
	
	{	
		firehoseDeliveryStreamName = System.getenv("deliveryStreamName");//properties.getProperty("firehose.DeliveryStreamName");
		firehoseEndpointUrl = System.getenv("firehoseEndpointUrl");
		
		firehoseClient = new AmazonKinesisFirehoseClient();
		firehoseClient.setEndpoint(firehoseEndpointUrl);
	}
	
	

	/**
		// Put Record Batch records. Max No.Of Records we can put in a
		// single put record batch request is 500
		// firehoseClient.putRecordBatch(putRecordBatchRequest);
	 **/
	@Override
	public String handleRequest(KinesisEvent event, Context context) {

		logger.info("Entry ");

		List<KinesisEventRecord> recordList = event.getRecords();

		System.out.println("Iterating through recordList of size "+recordList.size());
		logger.info("Iterating through recordList of size "+recordList.size());
		for(KinesisEventRecord rec:recordList){

			ByteBuffer data = rec.getKinesis().getData();
			
			try{
				
				
				writeToFirehose(ByteBuffer.wrap((new String(data.array())+"\n").getBytes()));
				
			}catch (AmazonServiceException ase) {
				
				ase.printStackTrace();
		        logger.error("Error Message:    " + ase.getMessage());
		        
		        return "Error due to "+ase.getErrorMessage();
		    } catch (AmazonClientException ace) {
		    	
		    	ace.printStackTrace();
		        logger.error("Error Message: " + ace.getMessage());
		        
		        
		        return "Error due to "+ace.getMessage();
		   
			} catch (Exception e) {
				
				e.printStackTrace();
				logger.error("Error Message: " + e.getMessage());
				
				
				return "Error due to "+e.getMessage();
			}
			
		}

		System.out.println("Iterating through recordList of size "+recordList.size()+"  complete !");
		//recordList.clear();

		return "Success!";
	}
	
	private void writeToFirehose(ByteBuffer data) {
		
		Record deliveryStreamRecord = new Record().withData (data);

		PutRecordRequest putRecordRequest = new PutRecordRequest()
				.withDeliveryStreamName(firehoseDeliveryStreamName)
				.withRecord(deliveryStreamRecord);
		
		logger.info("Putting message");

		System.out.println("Putting message");
		
		firehoseClient.putRecord(putRecordRequest);
	}
	
	}



