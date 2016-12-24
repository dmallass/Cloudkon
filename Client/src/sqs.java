
/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * This sample demonstrates how to make basic requests to Amazon SQS using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web
 * Services developer account, and be signed up to use Amazon SQS. For more
 * information on Amazon SQS, see http://aws.amazon.com/sqs.
 * <p>
 * Fill in your AWS access credentials in the provided credentials file
 * template, and be sure to move the file to the default location
 * (C:\\Users\\Ammu\\.aws\\credentials) where the sample code will load the credentials from.
 * <p>
 * <b>WARNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 */
public class sqs {
	
	public static String TABLE_NAME = "SQS_TABLE";
    public static void main(String[] args) throws Exception {

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\Ammu\\.aws\\credentials).
         */
        AWSCredentials credentials = null;
        try {
            //credentials = new ProfileCredentialsProvider("default").getCredentials();
        	
        	//AWSCredentials credentials = null;
            	 credentials = new PropertiesCredentials(
        				sqs.class
        						.getResourceAsStream("credentialsAWS.properties"));
        	
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\Ammu\\.aws\\credentials), and is in valid format.",
                    e);
        }

        AmazonSQS sqs = new AmazonSQSClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        sqs.setRegion(usWest2);

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon SQS");
        System.out.println("===========================================\n");
        
        AmazonDynamoDBClient db = dynamoService();
        createTable(db);
        
        try {
            // Create a queue
            //System.out.println("Creating a new SQS queue called RequestQueue.\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue");
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            
            //System.out.println("Creating a new SQS queue ResponseQueue.\n");
            CreateQueueRequest createQueueResponse = new CreateQueueRequest("taskQueue");
            String responseUrl = sqs.createQueue(createQueueResponse).getQueueUrl();
            
            //ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(responseUrl);
            //List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
           // String messageReceiptHandle = messages.get(0).getReceiptHandle();
        	//sqs.deleteMessage(new DeleteMessageRequest(responseUrl, messageReceiptHandle));
            // List queues
            //System.out.println("Listing all queues in your account.\n");
            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
                System.out.println("  QueueUrl: " + queueUrl);
            }
            System.out.println();
            
            // Send a message
            int i =0;
        	BufferedReader br = new BufferedReader(new FileReader("workload.txt"));
//            String arg0 = "workload.txt";
        	String line; 
        	String[] task = null;
        	while ((line = br.readLine()) != null) {
        		task = line.split(" ");
        		//int sleepTime = Integer.parseInt(task[1]);
        		//System.out.println("Sending a message to MyQueue.\n");
                sqs.sendMessage(new SendMessageRequest(myQueueUrl, task[1]));
                i++;
        	    }
            System.out.println("Sent "+i+ " messages sccessfully");
        }
        catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } 
        catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }         
    }
    
    public static AmazonDynamoDBClient dynamoService()
	{
		AWSCredentials credentials = null;
		AmazonDynamoDBClient dynamoDB;
        try {
            //credentials = new ProfileCredentialsProvider("default").getCredentials();
        	credentials = new PropertiesCredentials(
    				sqs.class
    						.getResourceAsStream("credentialsAWS.properties"));
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\Ammu\\.aws\\credentials), and is in valid format.",
                    e);
        }
        dynamoDB = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDB.setRegion(usWest2);
        return dynamoDB;
	}
    public static void createTable(AmazonDynamoDBClient dynamoDB) throws InterruptedException
    {
    	// Create table if it does not exist yet
    	if (Tables.doesTableExist(dynamoDB, TABLE_NAME)) {
            System.out.println("Table " + TABLE_NAME + " is already ACTIVE");
        } else {
            // Create a table with a primary hash key named 'name', which holds a string
            CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(TABLE_NAME)
                .withKeySchema(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName("Id").withAttributeType(ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
                TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
            System.out.println("Created Table: " + createdTableDescription);
            System.out.println("Waiting for " + TABLE_NAME + " to become ACTIVE...");
            Tables.awaitTableToBecomeActive(dynamoDB, TABLE_NAME);
            
        }

    }
    
    
}
