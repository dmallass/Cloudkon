import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class mainClass {

	private static int NO_THREADS = 1;
	private static String QUEUE_NAME = "MyQueue";
	private static String RESPONSE_QUEUE = "taskQueue";	
	public static String TABLE_NAME = "SQS_TABLE";
	public static GetQueueAttributesRequest a = new GetQueueAttributesRequest();
	private static String ATTR_NAME = "ApproximateNumberOfMessages";
	//private static int count = 0;
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		AmazonSQS sqs;
		AmazonDynamoDBClient dynamoDB;
		Map<String, String> map =  new HashMap<String,String>();
		sqs = sqsService();
		int requestQL = getQueueLength(sqs, QUEUE_NAME);
		int num = requestQL;
		//count = requestQL;
		System.out.println("Queue length"+num);
		dynamoDB = dynamoService();
		DynamoDB d = new DynamoDB(dynamoDB) ;
		d.createTable();
		//Thread.currentThread().wait(1000);
		

			/*for (int i=0; i<NO_THREADS ; i++)
			{
				pool.execute(new Worker(QUEUE_NAME,RESPONSE_QUEUE,sqs, a,dynamoDB,d));
			}
		             
		pool.shutdown();
		System.out.println("hey there");
		while(!pool.isTerminated()){ 
		}*/
		long startTime = System.currentTimeMillis();			
		execute(sqs,map,d,num);	
		long endTime = System.currentTimeMillis();
		long duration = endTime - startTime;
		System.out.println("RunTime  :"+ duration);
		int responseQL = getQueueLength(sqs, RESPONSE_QUEUE);
		if (responseQL == requestQL)
		{
			System.out.println("Finished Successfully");
		}
		//System.out.println("Deleting the test queue.\n");
        //sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));       
		
	}	
	
	public static AmazonDynamoDBClient dynamoService()
	{
		AWSCredentials credentials = null;
		AmazonDynamoDBClient dynamoDB;
        try {
            //credentials = new ProfileCredentialsProvider("default").getCredentials();
        	credentials = new PropertiesCredentials(
    				mainClass.class
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
	
	public static AmazonSQS sqsService()
	{
		AWSCredentials credentials = null;
		AmazonSQS sqs = null;
		try {
		  //credentials = new ProfileCredentialsProvider("default").getCredentials();
			credentials = new PropertiesCredentials(
    				mainClass.class
    						.getResourceAsStream("credentialsAWS.properties"));
		} catch (Exception e) {
		    throw new AmazonClientException(
		            "Cannot load the credentials from the credential profiles file. " +
		            "Please make sure that your credentials file is at the correct " +
		            "location (C:\\Users\\Ammu\\.aws\\credentials), and is in valid format.",
		            e);
		}
		    sqs = new AmazonSQSClient(credentials);
			Region usWest2 = Region.getRegion(Regions.US_WEST_2);
			sqs.setRegion(usWest2);		    
	return sqs;
	}
	
	
	public static int getQueueLength(AmazonSQS sqs, String name)
	{
		CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(name);
		a = new GetQueueAttributesRequest().withQueueUrl(sqs.createQueue(createQueueRequest).getQueueUrl());
		a.setAttributeNames(Arrays.asList( ATTR_NAME ));
		Map<String, String> map = sqs.getQueueAttributes(a).getAttributes();
		int num = Integer.parseInt(map.get("ApproximateNumberOfMessages"));
		return num;
	}
	
	
	public static void execute(AmazonSQS sqs,Map<String, String> map, DynamoDB d, int num)
	{
	GetQueueUrlResult responseUrl = sqs.getQueueUrl(RESPONSE_QUEUE);
	String url = responseUrl.toString();
	String[] urlTokens = url.substring(1, url.length()-1).split(" ");
					
	GetQueueUrlResult requestUrl = sqs.getQueueUrl(QUEUE_NAME);
	String requrl = requestUrl.toString();				
	String[] reqUrlTokens = requrl.substring(1, requrl.length()-1).split(" ");	
	int count = num;
	while(count>0)
		{
			List<Message> messages = receiveMessage(sqs);
			int sleepTime = 0; //boolean result = true;
			//System.out.println(num);
			for (Message message:messages)
			{
		        String id = message.getMessageId();
				String response = "True"+":"+ id;
		        sleepTime = Integer.parseInt(message.getBody()); 
		        //System.out.println("Message ID"+id);		        
		        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		        //item.put(id, new AttributeValue().withS(message.getBody()));
		        item.put("Id", new AttributeValue().withS(id));
		        boolean result = d.getItem(item, id,message.getBody());
		        if(result == false)
		        {		        	
		        	//System.out.println("Item not present");
		        	d.addItem(id, message.getBody());
		        	try {
						Thread.sleep(sleepTime);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					sqs.sendMessage(new SendMessageRequest(urlTokens[1],response));
       
		        }		        						
		        	String messageReceiptHandle = messages.get(0).getReceiptHandle();
		        	sqs.deleteMessage(new DeleteMessageRequest(reqUrlTokens[1], messageReceiptHandle));
		        	//map = sqs.getQueueAttributes(a).getAttributes();
					//num = Integer.parseInt(map.get("ApproximateNumberOfMessages"));
					count--;
				} 
		     
			if(count == 0)
				break;
			}
		
		}
		public static List<Message> receiveMessage(AmazonSQS sqs)
		{
		    ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(QUEUE_NAME);
		    List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		    return messages;
		}
	
}
	
	
