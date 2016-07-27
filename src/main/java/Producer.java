/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class Producer {

    // Set the number of messages to send.
    public static int numMessages = 60;
    // Declare a new producer
    public static KafkaProducer producer;

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage:\n" +
                    "\tjava -cp ms-sparkstreaming-1.0.jar:`mapr classpath` Producer [data file] [stream:topic]\n" +
                    "Example:\n" +
                    "\tjava -cp ms-sparkstreaming-1.0.jar:`mapr classpath` Producer /mapr/demo.mapr.com/data/taqtrade20131218 /user/mapr/taq:trades");
            throw new IllegalArgumentException("ERROR: You must specify the topic and input file.");
        }

        String topic =  args[1] ;
        System.out.println("Publishing to topic: "+ topic);

        configureProducer();
        System.out.println("Opening file " + args[0]);
        File f = new File(args[0]);
        FileReader fr = new FileReader(f);
        BufferedReader reader = new BufferedReader(fr);
        String line = reader.readLine();
        long linecount = 0L;
        long bytescount = 0L;

        long startTime = System.nanoTime();
        while (line != null) {
//            String[] temp = line.split(",");
//            String key=temp[0];
            /* Add each message to a record. A ProducerRecord object
             identifies the topic or specific partition to publish
             a message to. */
//            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic,key,line);
//            System.out.println("Preparing to publish: " + line);
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic,line);

            // Send the record to the producer client library.
            producer.send(rec);
//            System.out.println("Published this line:\t" + rec.toString());
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            linecount ++;

            if (linecount % 1000000 == 0) {
//                System.out.print(linecount/1000000 + "M lines published.\r");
                long endTime = System.nanoTime();
                long elapsedTime = endTime - startTime;

                System.out.format("Throughput = %.2f Kmsgs/sec published. Total published = %d\n", linecount/((double)elapsedTime/1000000000.0)/1000, linecount);

            }
            line = reader.readLine();
        }

        producer.close();
        System.out.println("Published " + linecount + " messages to stream.");
        System.out.println("Finished.");

        System.exit(1);

    }

    /* Set the value for a configuration parameter.
     This configuration parameter specifies which class
     to use to serialize the value of each message.*/
    public static void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }

}
