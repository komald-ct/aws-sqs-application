package org.ct.aws_sqs_application.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ct.aws_sqs_application.SQSDemo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * @author kunalp3
 * This class manages the creation of queue on sqs client,
 * sending the message, receiving the message and deleting the message
 * on receive receipt
 */

@Service
public class SQSServiceImpl {

	static AmazonSQSClient sqsClient = SQSDemo.getSQSInstance(); 
	static Logger log = LoggerFactory.getLogger(SQSServiceImpl.class);

	/**
	 * This method takes Queue name as input parameter and creates a
	 * Standard queue on aws SQS instance and return success message
	 * if queue instance is created.
	 * @param QUEUE_NAME
	 * @return Success message
	 * 
	 * Deprecated this api as user should not be allowed to create queue
	 */
	@Deprecated
	public String createStdQueue(String QUEUE_NAME) {
		CreateQueueRequest create_request = new CreateQueueRequest(QUEUE_NAME)
				.addAttributesEntry("DelaySeconds", "60")
				.addAttributesEntry("MessageRetentionPeriod", "86400");

		//Creating new Queue here, returning success if true already exists
		try {
			sqsClient.createQueue(create_request);
		} catch (AmazonSQSException e) {
			if (!e.getErrorCode().equals("QueueAlreadyExists")) {
				throw e;
			}
		}
		log.info("The Standard Queue has been created with name "+QUEUE_NAME);
		return "The Standard Queue with name "+QUEUE_NAME+" has been created.";
	}

	/**
	 * This method takes Queue name as input parameter and creates a
	 * FIFO queue on aws SQS instance and return success message
	 * if queue instance is created.
	 * @param QUEUE_NAME
	 * @return Success message
	 * 
	 * Deprecated this api as user should not be allowed to create queue
	 */
	@Deprecated
	public String createFIFOQueue(String QUEUE_NAME) {

		Map<String, String> attributes = new HashMap<String, String>();
		// A FIFO queue must have the FifoQueue attribute set to True
		attributes.put("FifoQueue", "true");
		// Generate a MessageDeduplicationId based on the content, 
		//if the user doesn't provide a MessageDeduplicationId
		attributes.put("ContentBasedDeduplication", "true");
		// The FIFO queue name must end with the .fifo suffix
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_NAME+".fifo")
				.withAttributes(attributes);
		String myQueueUrl = sqsClient.createQueue(createQueueRequest).getQueueUrl();

		log.info("The FIFO Queue has been created with name "+QUEUE_NAME);
		log.info("The FIFO Queue url is: "+myQueueUrl);
		return "The FIFO Queue with name "+QUEUE_NAME+" has been created.";
	}

	/**
	 * This method takes standard Queue name as input parameter and sends a
	 * message on that standard queue, returns success message if message is posted.
	 * if queue instance is created.
	 * @param QUEUE_NAME, message in payload
	 * @return Success message
	 */
	public String sendMessageOnStdQueue(String QUEUE_NAME, String message) {

		//Fetching the queue url from queue name
		log.info("The queue name of queue is: "+QUEUE_NAME+".fifo");
		String queueUrl= sqsClient.getQueueUrl(QUEUE_NAME).getQueueUrl();
		log.info("The queue url fetched is: "+queueUrl);        
		SendMessageRequest send_msg_request = new SendMessageRequest()
				.withQueueUrl(queueUrl)
				.withMessageBody(message)
				.withDelaySeconds(5);
		sqsClient.sendMessage(send_msg_request);

		log.info("The message has been sent.");
		return "The message has been sent.";
	}

	/**
	 * This method takes FIFO Queue name as input parameter and sends a
	 * message on that FIFO queue, returns success message if message is posted.
	 * if queue instance is created.
	 * @param QUEUE_NAME, message in payload
	 * @return Success message
	 */
	public String sendMessageOnFIFOQueue(String QUEUE_NAME, String message) {

		//Fetching the queue url from queue name
		log.info("The queue name queue is: "+QUEUE_NAME+".fifo");   
		String queueUrl= sqsClient.getQueueUrl(QUEUE_NAME+".fifo").getQueueUrl();
		log.info("The queue url fetched is: "+queueUrl);        
		SendMessageRequest send_msg_request = new SendMessageRequest()
				.withQueueUrl(queueUrl)
				.withMessageBody(message);
		send_msg_request.setMessageGroupId("messageGroup1");
		sqsClient.sendMessage(send_msg_request);

		log.info("The message has been sent.");
		return "The message has been sent.";
	}

	/**
	 * This method simply returns the queues available for the logged in user
	 * for that particular region
	 * @return List of Queues
	 */
	public List<String> getListOfQueuesAvailable() {

		ListQueuesResult queueList = sqsClient.listQueues();
		log.info("The Queues available are: "+ queueList);
		return queueList.getQueueUrls();
	}

	/**
	 * This method returns a messages from standard queue requested
	 * @param Queue name
	 * @return a message from standard queue
	 */
	public String getMsgsFromStdQueue(String QUEUE_NAME) {

		log.info("Requested a messages from the Queue : "+ QUEUE_NAME);

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsClient.getQueueUrl(QUEUE_NAME).getQueueUrl());
		List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
		if(!messages.isEmpty()) {
			Message msg = new Message();
			msg = messages.get(0);
			sqsClient.deleteMessage(sqsClient.getQueueUrl(QUEUE_NAME).getQueueUrl(), msg.getReceiptHandle());
			return msg.getBody();
		}
		else {
			log.info("There are no messages for now");
			return "There are no messages for now";
		}
	}

	/**
	 * This method returns a message from FIFO queue requested
	 * @param Queue name
	 * @return a message from FIFO queue
	 */
	public String getMsgsFromFIFOQueue(String QUEUE_NAME) {

		log.info("Requested a message from the Queue : "+ QUEUE_NAME +".fifo");

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsClient.getQueueUrl(QUEUE_NAME+".fifo").getQueueUrl());
		List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
		if(!messages.isEmpty()) {
			Message msg = new Message();
			msg = messages.get(0);
			sqsClient.deleteMessage(sqsClient.getQueueUrl(QUEUE_NAME).getQueueUrl(), msg.getReceiptHandle());
			return msg.getBody();
		}
		else {
			log.info("There are no messages for now");
			return "There are no messages for now";
		}
	}

}
