package com.amazonaws.lambda.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {
	final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
	final   AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
	public String handleRequest(S3Event event, Context context) {
		String table_name="Person";
        context.getLogger().log("Received event: " + event);
        context.getLogger().log("Triggered from S3 and executing the function ");
        S3EventNotificationRecord record = event.getRecords().get(0);
        String srcFileName = record.getS3().getObject().getKey();
        String key =         event.getRecords().get(0).getS3().getObject().getKey();    
        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        context.getLogger().log("srcFileName-->"+srcFileName+" *key---->"+key+" *bucket--->"+bucket);
        //The below line is used to get the value from the environment variable
        //These env variables are declared during the Lambda function.
        context.getLogger().log("Secret Key------->"+System.getenv("secreyKey"));
        S3Object object = s3.getObject(new GetObjectRequest(bucket, srcFileName));
        try {
			displayTextInputStream(object.getObjectContent(),context);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Map<String,AttributeValue> map = new HashMap<String,AttributeValue>();
        map.put("FirstName", new AttributeValue("Anil13"));
        map.put("LastName", new AttributeValue("Kumar13"));
        context.getLogger().log("The request map is-------->"+map.toString());
       PutItemResult result  =  ddb.putItem(table_name,map,ReturnValue.ALL_OLD.name());
       context.getLogger().log("Executed Successfully from Lambda--->"+result.getAttributes());
        	return "The bucket name is--"+bucket+"----and the key file is----"+key+"---and the source file name is--"+srcFileName;
    }
	 private  void displayTextInputStream(InputStream input,Context context) throws IOException {
	        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
	        while (true) {
	            String line = reader.readLine();
	            if (line == null) break;
	            context.getLogger().log("Line is -------->"+line);
	        }
	    }
}
