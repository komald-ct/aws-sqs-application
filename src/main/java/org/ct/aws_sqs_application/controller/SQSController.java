package org.ct.aws_sqs_application.controller;

import java.util.List;

import org.ct.aws_sqs_application.service.SQSServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author kunalp3
 * This class acts as controller for the rest endpoints 
 * for below mentioned functionalities,
 * Create standard queue - Code is commented
 * Create fifo queue - Code is commented
 * Send message on standard queue
 * Send message on fifo queue
 * Get list of all the queues available under region of the user
 * Read message from standard queue
 * Read message from fifo queue 
 */

@RestController
public class SQSController {

	@Autowired
	SQSServiceImpl service;
	
/*	@RequestMapping(value="/createstd/{QUEUE_NAME}", method=RequestMethod.POST)
	public String createStdQueue(@PathVariable String QUEUE_NAME) {
		
		return service.createStdQueue(QUEUE_NAME);
	}*/
	
	
/*	@RequestMapping(value="/createfifo/{QUEUE_NAME}", method=RequestMethod.POST)
	public String createFIFOQueue(@PathVariable String QUEUE_NAME) {
		
		return service.createFIFOQueue(QUEUE_NAME);
	}*/
	
	@RequestMapping(value="/sendmsgstd/{QUEUE_NAME}", method=RequestMethod.POST)
	public String sendMessageOnStdQueue(@PathVariable String QUEUE_NAME, @RequestBody String message) {
		
		return service.sendMessageOnStdQueue(QUEUE_NAME, message);
	}
	
	@RequestMapping(value="/sendmsgfifo/{QUEUE_NAME}", method=RequestMethod.POST)
	public String sendMessageOnFIFOQueue(@PathVariable String QUEUE_NAME, @RequestBody String message) {
		
		return service.sendMessageOnFIFOQueue(QUEUE_NAME, message);
	}
	
	@RequestMapping("/queues")
	public List<String> getListOfQueuesAvailable() {
		
		return service.getListOfQueuesAvailable();
	}
	
	@RequestMapping("stdqueue/message/{QUEUE_NAME}")
	public String getMsgsFromStdQueue(@PathVariable String QUEUE_NAME) {
		
		return service.getMsgsFromStdQueue(QUEUE_NAME);
	}
	
	
	@RequestMapping("fifoqueue/message/{QUEUE_NAME}")
	public String getMsgsFromFIFOQueue(@PathVariable String QUEUE_NAME) {
		
		return service.getMsgsFromFIFOQueue(QUEUE_NAME);
	}
}
