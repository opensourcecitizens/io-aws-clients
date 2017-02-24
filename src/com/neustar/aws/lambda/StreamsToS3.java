package com.neustar.aws.lambda;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.StorageClass;
import com.google.common.io.Resources;

import io.parser.utils.JsonUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

public class StreamsToS3 implements RequestHandler<KinesisEvent, String>{

	private String bucketName =null;
	private String guidKey = null;
	private Properties properties = null;
	{
		
		/*try{
		InputStream props = Resources.getResource("aws_producer.props").openStream();
		properties = new Properties();
		properties.load(props);
		}catch(Exception e){
			
		}*/
		bucketName = System.getenv("bucketName");// properties.getProperty("bucketName");
		guidKey = System.getenv("guidKey");// properties.getProperty("guidKey");
		
	}
	
	private AmazonS3Client s3Client = new AmazonS3Client();

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
				JsonUtils utils = new JsonUtils();
				Map<String,?> mappedData  = utils.parseJsonData(data.array());
				guidValue = utils.searchMap(guidKey, mappedData);
				
				logger.log("guidValue = "+guidValue);
				
				saveToS3(new String(data.array()), guidValue.toString(), logger);
				
			} catch (AmazonServiceException ase) {

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

		return "Success";
	}
	
	private void saveToS3(String jsonDocument, String keyName,
	        LambdaLogger logger) {
	    	
	    	AmazonS3Client s3client = new AmazonS3Client(
	            new DefaultAWSCredentialsProviderChain());

	    
	        logger.log("Uploading a new object to S3 from a file\n");

	        InputStream stream = new ByteArrayInputStream(
	                jsonDocument.getBytes(StandardCharsets.UTF_8));
	        String bucket = bucketName;
	        ObjectMetadata metadata = new ObjectMetadata();
	        metadata.setContentType("application/json");
	        metadata.setContentDisposition("attachment; filename=\"" + keyName);
	        PutObjectRequest put = new PutObjectRequest(bucket, keyName,
	                stream, metadata);
	        put.setStorageClass(StorageClass.ReducedRedundancy);
	        put.setMetadata(metadata);
	        s3client.putObject(put);

	}


}
