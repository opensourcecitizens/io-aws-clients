package com.neustar.aws.lambda;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

public class StreamToFirehose implements RequestHandler<KinesisEvent, String>{

	private String firehoseDeliveryStreamName =null;
	private Properties properties = null;
	
	
	{
		/*try{
		InputStream props = Resources.getResource("aws_lamda_StreamToFirehose.props").openStream();
		properties = new Properties();
		properties.load(props);
		}catch(Exception e){
			
		}*/
		
		firehoseDeliveryStreamName = System.getenv("deliveryStreamName");//properties.getProperty("firehose.DeliveryStreamName");
	}
	
	private AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient();

	/**
		// Put Record Batch records. Max No.Of Records we can put in a
		// single put record batch request is 500
		// firehoseClient.putRecordBatch(putRecordBatchRequest);
	 **/
	@Override
	public String handleRequest(KinesisEvent event, Context context) {
		
		
		LambdaLogger logger = context.getLogger();

		List<KinesisEventRecord> recordList = event.getRecords();

		for(KinesisEventRecord rec:recordList){

			//String msg = new String(rec.getKinesis().getData().array());
			ByteBuffer data = rec.getKinesis().getData();

			//Record deliveryStreamRecord = new Record().withData (ByteBuffer.wrap(msg.getBytes()));

			try{
				writeToFirehose(data,logger);
			}catch (AmazonServiceException ase) {

		        logger.log("Error Message:    " + ase.getMessage());
		        
		        return "Error due to "+ase.getErrorMessage();
		    } catch (AmazonClientException ace) {

		        logger.log("Error Message: " + ace.getMessage());
		        
		        return "Error due to "+ace.getMessage();
		   
			} catch (Exception e) {
				
				logger.log("Error Message: " + e.getMessage());
				
				return "Error due to "+e.getMessage();
			}
			
		}

		recordList.clear();

		return "Success!";
	}
	
	private void writeToFirehose(ByteBuffer data,
	        LambdaLogger logger) {
		
		Record deliveryStreamRecord = new Record().withData (data);

		PutRecordRequest putRecordRequest = new PutRecordRequest()
				.withDeliveryStreamName(firehoseDeliveryStreamName)
				.withRecord(deliveryStreamRecord);

		logger.log("Putting message");

		firehoseClient.putRecord(putRecordRequest);
	}
	
	}



