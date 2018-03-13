package com.gpte.redhat;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

public class Sender {
    private static final int DEFAULT_COUNT = 10;
    private static final int DELIVERY_MODE = DeliveryMode.NON_PERSISTENT;

    public static void main(String[] args) throws Exception {
        int count = DEFAULT_COUNT;
        if (args.length == 0) {
            System.out.println("Sending up to " + count + " messages.");
            System.out.println("Specify a message count as the program argument if you wish to send a different amount.");
        } else {
            count = Integer.parseInt(args[0]);
            System.out.println("Sending up to " + count + " messages.");
        }
        
        while (true) {

            try {
                // The configuration for the Qpid InitialContextFactory has been supplied in
                // a jndi.properties file in the classpath, which results in it being picked
                // up automatically by the InitialContext constructor.
                Context context = new InitialContext();

                ConnectionFactory factory = (ConnectionFactory) context.lookup("myFactoryLookup");
                Destination queue = (Destination) context.lookup("myQueueLookup");

                Connection connection = factory.createConnection(System.getProperty("USER"), System.getProperty("PASSWORD"));
                connection.setExceptionListener(new MyExceptionListener());
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                MessageProducer messageProducer = session.createProducer(queue);

                long start = System.currentTimeMillis();
                for (int i = 1; i <= count; i++) {
                    TextMessage message = session.createTextMessage("Text!");
                    messageProducer.send(message, DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

                    if (i % 100 == 0) {
                        System.out.println("Sent message " + i);
                    }
                }

                long finish = System.currentTimeMillis();
                long taken = finish - start;
                System.out.println("Sent " + count + " messages in " + taken + "ms");

                connection.close();
                Thread.sleep(1000);
            } catch (Exception exp) {
                System.out.println("Caught exception, sleeping for a bit.");
                exp.printStackTrace(System.out);
                Thread.sleep(5000);
            }
        }
    }

    private static class MyExceptionListener implements ExceptionListener {

		public void onException(JMSException exception) {
			System.out.println("Connection ExceptionListener fired, exiting.");
            exception.printStackTrace(System.out);
            System.exit(1);
			
		}

    }
}
