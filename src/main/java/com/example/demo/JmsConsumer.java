

// SCCSID "@(#) MQMBID sn=p913-L190628 su=_YwDYBZmUEemAId1m26z03A pn=MQJavaSamples/jms/JmsConsumer.java"
/*
 *   <copyright
 *   notice="lm-source-program"
 *   pids="5724-H72,5655-R36,5655-L82,5724-L26"
 *   years="2008,2014"
 *   crc="39457954" >
 *   Licensed Materials - Property of IBM
 *
 *   5724-H72,5655-R36,5655-L82,5724-L26
 *
 *   (C) Copyright IBM Corp. 2008, 2014 All Rights Reserved.
 *
 *   US Government Users Restricted Rights - Use, duplication or
 *   disclosure restricted by GSA ADP Schedule Contract with
 *   IBM Corp.
 *   </copyright>
 */
package com.example.demo;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import lombok.extern.slf4j.Slf4j;

/**
 * A JMS consumer (receiver or subscriber) application that receives a message from the named
 * destination (queue or topic).
 *
 * Tip: A subscriber application must be started before the publisher application.
 *
 * Notes:
 *
 * API type: IBM JMS API (v1.1, unified domain)
 *
 * Messaging domain: Point-to-point or Publish-Subscribe
 *
 * Provider type: WebSphere MQ
 *
 * Connection mode: Client connection
 *
 * JNDI in use: No
 *
 * Usage:
 *
 * JmsConsumer -m queueManagerName -d destinationName [-h host -p port -l channel] [-u user -w passWord] [-t timeoutSeconds]
 *
 * for example:
 *
 * JmsConsumer -m QM1 -d Q1
 *
 * JmsConsumer -m QM1 -d topic://foo -h localhost -p 1414 -u tester -w testpw
 */
@Slf4j
public class JmsConsumer {

	private static String host = "localhost";
	private static int port = 1414;
	private static String channel = "SYSTEM.DEF.SVRCONN";
	private static String user = null;
	private static String password = null;
	private static String queueManagerName = null;
	private static String destinationName = null;
	private static boolean isTopic = false;
	private static boolean clientTransport = true;

	private static int timeout = 3000000; // in ms or 15 seconds

	// System exit status value (assume unset value to be 1)
	private static int status = 1;

	/**
	 * Main method
	 *
	 * @param args
	 */
	public static void main(String[] args) {
		// Parse the arguments
		parseArgs(args);

		// Variables
		Connection connection = null;
		Session session = null;
		Destination destination = null;
		MessageConsumer consumer = null;
		TopicSubscriber subscriber = null;

		try {
			// Create a connection factory
			JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
			JmsConnectionFactory cf = ff.createConnectionFactory();

			// Set the properties
			cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, host);
			cf.setIntProperty(WMQConstants.WMQ_PORT, port);
			cf.setStringProperty(WMQConstants.WMQ_CHANNEL, channel);
			cf.setStringProperty(WMQConstants.CLIENT_ID, "D-001");
			if (clientTransport) {
				cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
			}
			else {
				cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_BINDINGS);
			}
			cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, queueManagerName);
			if (user != null) {
				cf.setStringProperty(WMQConstants.USERID, user);
				cf.setStringProperty(WMQConstants.PASSWORD, password);
				cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
			}

			//Print the properties
			log.info("JMS ConnectionFactory properties are {}", cf);

			// Create JMS objects
			connection = cf.createConnection();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			if (isTopic) {
//				destination = session.createTopic(destinationName);
				Topic topic  = session.createTopic(destinationName);
				subscriber = session.createDurableSubscriber(topic,"D-001");
			}
			else {
				destination = session.createQueue(destinationName);
			}
			log.info("Session object is {}",session);
//			consumer = session.createConsumer(destination);

			// Start the connection
			connection.start();

			// And, receive the message
			Message message;
			do {
				message = subscriber.receive(timeout);
				if (message != null) {
					log.info("Received message:\n" + message);
				}
			}
			while (message != null);

			log.error("No message received in {} seconds!\n", timeout / 1000);
			recordSuccess();
		}
		catch (JMSException jmsex) {
			recordFailure(jmsex);
		}
		finally {
			if (consumer != null) {
				try {
					consumer.close();
				}
				catch (JMSException jmsex) {
					log.error("Consumer could not be closed.");
					recordFailure(jmsex);
				}
			}

			if (session != null) {
				try {
					session.close();
				}
				catch (JMSException jmsex) {
					log.error("Session could not be closed.");
					recordFailure(jmsex);
				}
			}

			if (connection != null) {
				try {
					connection.close();
				}
				catch (JMSException jmsex) {
					log.error("Connection could not be closed.");
					recordFailure(jmsex);
				}
			}
		}
		System.exit(status);
	} // end main()

	/**
	 * Process a JMSException and any associated inner exceptions.
	 *
	 * @param jmsex
	 */
	private static void processJMSException(JMSException jmsex) {
		log.debug("jmsex is {} ", jmsex.toString());
		Throwable innerException = jmsex.getLinkedException();
		if (innerException != null) {
			log.error("Inner exception(s):");
		}
		while (innerException != null) {
			log.error("Inner exception  {}",innerException.toString());
			innerException = innerException.getCause();
		}
	}

	/**
	 * Record this run as successful.
	 */
	private static void recordSuccess() {
		log.info("SUCCESS");
		status = 0;
	}

	/**
	 * Record this run as failure.
	 *
	 * @param ex
	 */
	private static void recordFailure(Exception ex) {
		if (ex != null) {
			if (ex instanceof JMSException) {
				processJMSException((JMSException) ex);
			}
			else {
				log.error("The exception is :: {}" ,ex.toString());
			}
		}
		log.error("FAILURE");
		status = -1;
	}

	/**
	 * Parse user supplied arguments.
	 *
	 * @param args
	 */
	private static void parseArgs(String[] args) {
		try {
			int length = args.length;
			log.debug("Length is ::"+ length);
			if (length == 0) {
				throw new IllegalArgumentException("No arguments! Mandatory arguments must be specified.");
			}
			if ((length % 2) != 0) {
				throw new IllegalArgumentException("Incorrect number of arguments!");
			}

			int i = 0;

			while (i < length) {
				if ((args[i]).charAt(0) != '-') {
					throw new IllegalArgumentException("Expected a '-' character next: " + args[i]);
				}

				char opt = (args[i]).toLowerCase().charAt(1);

				switch (opt) {
					case 'h' :
						host = args[++i];
						clientTransport = true;
						break;
					case 'p' :
						port = Integer.parseInt(args[++i]);
						break;
					case 'l' :
						channel = args[++i];
						break;
					case 'm' :
						queueManagerName = args[++i];
						break;
					case 'd' :
						destinationName = args[++i];
						break;
					case 'u' :
						user = args[++i];
						break;
					case 'w' :
						password = args[++i];
						break;
					case 't' :
						i = timeoutValidation(args, i);
						break;
					default : {
						throw new IllegalArgumentException("Unknown argument: " + opt);
					}
				}

				++i;
			}

			if (queueManagerName == null) {
				throw new IllegalArgumentException("A queueManager name must be specified.");
			}

			if (destinationName == null) {
				throw new IllegalArgumentException("A destination name must be specified.");
			}

			if (((user == null) && (password != null)) ||
					((user != null) && (password == null))) {
				throw new IllegalArgumentException("A userid and password must be specified together");
			}

			// Whether the destination is a queue or a topic. Apply a simple check.
			if (destinationName.startsWith("topic://")) {
				isTopic = true;
			}
			else {
				// Otherwise, let's assume it is a queue.
				isTopic = false;
			}
		}
		catch (Exception e) {
			log.error(e.getMessage());
			printUsage();
			System.exit(-1);
		}
	}

	private static int timeoutValidation(String[] args, int i) {
		try {
			int timeoutSeconds = Integer.parseInt(args[++i]);
			timeout = timeoutSeconds * 1000;
		}
		catch (NumberFormatException nfe) {
			throw new IllegalArgumentException("Timeout must be a whole number of seconds");
		}
		return i;
	}

	/**
	 * Display usage help.
	 */
	private static void printUsage() {
		log.info("\nUsage:");
		log.info("JmsConsumer -m queueManagerName -d destinationName [-h host -p port -l channel] [-u user -w passWord] [-t timeout_seconds]");
	}

} // end class
