package com.neustar.aws.kinesis;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.util.StringUtils;
import com.google.common.io.Resources;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamResult;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Sends messages to specified Kinesis Stream in aws_producer.props resource.
 * This is dependent on credentials file which has implications on kinesis stream instance on AWS and region.
 * @param <T>
 * @param <T>
 */

/*
public class KinesisFirehoseProducerClient<T>{
	private  static final Logger log = Logger.getLogger(KinesisFirehoseProducerClient.class);
	
	
	private static KinesisFirehoseProducerClient<?> dsproducer = null;

	public static KinesisFirehoseProducerClient<?> singleton() throws IOException {
		if (dsproducer == null) {
			dsproducer = new KinesisFirehoseProducerClient();
		}
		return dsproducer;
	}

	//private KafkaProducer<String, T> producer = null;
	private static AmazonKinesisFirehoseClient kinesis;
	private String topic = null;
	private Properties properties = null;
	private String myStreamName = null;
	private String region = null;

	public KinesisFirehoseProducerClient() throws IOException {
		InputStream props = Resources.getResource("aws_producer.props").openStream();
		properties = new Properties();
		properties.load(props);
		//producer = new KafkaProducer<String, T>(properties);
		
		
        AWSCredentials credentials = null;
        
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        kinesis = new AmazonKinesisFirehoseClient(credentials);

	}
	
	public KinesisFirehoseProducerClient(AWSCredentials credentials) throws IOException {
		InputStream props = Resources.getResource("aws_producer.props").openStream();
		properties = new Properties();
		properties.load(props);
		
		 kinesis = new AmazonKinesisFirehoseClient(credentials);
	}

	public void send(byte[] message) {
		
		//topic = properties.getProperty("topic.id");
		myStreamName = properties.getProperty("firehose.id");
		region = properties.getProperty("region.id")==null?"us_west_2":properties.getProperty("region.id");
		// Describe the stream and check if it exists.
        CreateDeliveryStreamRequest describeStreamRequest = new CreateDeliveryStreamRequest().withDeliveryStreamName(myStreamName);
        
        try {
        	
        	kinesis.configureRegion(Regions.valueOf(StringUtils.upperCase(region)));
        	
            CreateDeliveryStreamResult streamDescription = kinesis.createDeliveryStream(describeStreamRequest);
            System.out.printf("Stream %s has a status of %s.\n", myStreamName, streamDescription.getStreamStatus());

            if ("DELETING".equals(streamDescription.getStreamStatus())) {
                System.out.println("Stream is being deleted. This sample will now exit.");
                System.exit(0);
            }

            // Wait for the stream to become active if it is not yet ACTIVE.
            if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
                waitForStreamToBecomeAvailable(myStreamName);
            }
            
        } catch (ResourceNotFoundException ex) {
        	//ex.printStackTrace();
        	System.out.println("Stream not found .... exiting  cause: "+ex.getErrorMessage());
        	System.exit(0);
        	
        } catch (InterruptedException e) {
        	
			e.printStackTrace();
			
		}

		//topic = properties.getProperty("topic.id");
		//Future<RecordMetadata> threadFuture = producer.send(new ProducerRecord<String, T>(topic, message));		
		//return threadFuture;
        
        System.out.printf("Putting records in stream : %s until this application is stopped...\n", myStreamName);
        // System.out.println("Press CTRL-C to stop.");
        // Write records to the stream until this program is aborted.
        //while (true) {
            long createTime = System.currentTimeMillis();
            
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(myStreamName);
            putRecordRequest.setData(ByteBuffer.wrap(message));
            //putRecordRequest.setPartitionKey(String.format("partitionKey-%d", createTime));
            //putRecordRequest.setSequenceNumberForOrdering(System.currentTimeMillis()+"");
            
            PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
            
            System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
            putRecordRequest.getPartitionKey(),
            putRecordResult.getShardId(),
            putRecordResult.getSequenceNumber());
        //}

	}


    private void waitForStreamToBecomeAvailable(String myStreamName) throws InterruptedException {
        System.out.printf("Waiting for %s to become ACTIVE...\n", myStreamName);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(myStreamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesis.describeDeliveryStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                System.out.printf("\t- current state: %s\n", streamStatus);
                if ("ACTIVE".equals(streamStatus)) {
                    return;
                }
            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            } catch (AmazonServiceException ase) {
                throw ase;
            }
        }

        throw new RuntimeException(String.format("Stream %s never became active", myStreamName));
    }
    

	public void finalize() throws Throwable {
		super.finalize();
	}

}
*/
