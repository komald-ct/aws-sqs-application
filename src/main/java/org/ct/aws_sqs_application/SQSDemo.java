package org.ct.aws_sqs_application;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClient;

/**
 * @author kunalp3
 * This application demonstrate connection to AWS SimpleQueueService
 * The methods in the service class will create the Queue/s on aws,
 * post the message on queue, extract the queues for that region and
 * retrieve the message along with delete on retrieval.
 */

@SpringBootApplication
public class SQSDemo 
{
	private static Properties properties = new Properties();
	
    static Logger log = LoggerFactory.getLogger(SQSDemo.class);
    public static void main( String[] args )
    {
    	SpringApplication.run(SQSDemo.class, args);
    }
    
    /**
     * This method will authenticate the user based on access key and secrete key 
     * provided and will create and return the instance of sqsclient
     * @return SQS client instance
     */
    public static AmazonSQSClient getSQSInstance() {
    	
    	try {
    	properties.load(new FileInputStream("D:/awsConfig/credentials.properties"));
    	}catch(IOException e){
    		e.printStackTrace();
    	}
    
    	String ACCESS_KEY = properties.getProperty("ACCESS_KEY");
    	String SECRET_KEY = properties.getProperty("SECRET_KEY");

    	BasicAWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);
    	log.info("Login succeed");
    	AmazonSQSClient sqsClient = new AmazonSQSClient(credentials).withRegion(Regions.US_EAST_2);
    	log.info("SQS client instance is created.");
    	return sqsClient;
    }
}
