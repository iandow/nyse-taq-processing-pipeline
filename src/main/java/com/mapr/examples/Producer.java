package com.mapr.examples;/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.IOException;
import java.util.Properties;

public class Producer {

    public static KafkaProducer producer;

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer [source data file] [stream:topic]\n" +
                    "Example:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer data/taqtrade20131218 /usr/mapr/taq:trades");
            throw new IllegalArgumentException("ERROR: You must specify the input data file and stream:topic.");
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

        try {
            long startTime = System.nanoTime();
            while (line != null) {
                ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, line);

                // Send the record to the producer client library.
                producer.send(rec);
                linecount++;

                if (linecount % 1000000 == 0) {
                    producer.flush();
                    long endTime = System.nanoTime();
                    long elapsedTime = endTime - startTime;
                    System.out.printf("Throughput = %.2f Kmsgs/sec published. Total published = %d\n", linecount / ((double) elapsedTime / 1000000000.0) / 1000, linecount);
                }
                line = reader.readLine();
            }
        } catch (Throwable throwable) {
            System.err.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
            System.out.println("Published " + linecount + " messages to stream.");
            System.out.println("Finished.");
        }
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
