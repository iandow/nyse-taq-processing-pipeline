package com.mapr.examples;/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

import java.text.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.util.*;
import java.util.concurrent.*;

public class Consumer {

    // Declare a new consumer.
    public static KafkaConsumer consumer;

    public static void main(String[] args) {
        Runtime runtime = Runtime.getRuntime();
        if (args.length < 2) {
            System.err.println("ERROR: You must specify a stream:topic to consume data from.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer [stream:topic]\n" +
                    "Example:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer  /usr/mapr/taq:trades");

        }

        String topic =  args[1] ;
        System.out.println("Subscribed to : "+ topic);

        configureConsumer();

        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        // Subscribe to the topic.
        consumer.subscribe(topics);

        int concurrencyFactor = 5;
        int poolSize = concurrencyFactor * Runtime.getRuntime().availableProcessors();
        ExecutorService es = Executors.newFixedThreadPool(poolSize);
        CompletionService<Boolean> completionService = new ExecutorCompletionService(es);

        List<String> buffer = new ArrayList<>();
        List<ConsumerRecord> recordList = new ArrayList();
        long pollTimeOut = 10000;  // milliseconds
        long records_processed = 0L;
        final int minBatchSize = 200;

        // https://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
        // This paradigm is an "at least once delivery" guarantee.
        // TODO: Is it okay for the listener #1 to potentially persist duplicate messages?

        long startTime = System.nanoTime();
        long last_update = 0;

        try {
            while (true) {
                // Request unread messages from the topic.
                ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
                if (records.count() == 0) {
                    System.out.println("No messages after " + pollTimeOut/1000 + " second wait. Total published = " + records_processed);
                } else {

                    for (ConsumerRecord<String, String> record : records) {
                        recordList.add(record);
                        //collect records
                        if (recordList.size() == poolSize) {
                            int taskCount = poolSize;
                            //distribute these messages across the workers
                            recordList.forEach(recordTobeProcess -> completionService.submit(new Worker(recordTobeProcess)));
                            //collect the processing result
                            List<Boolean> resultList = new ArrayList();
                            while (taskCount > 0) {
                                try {
                                    Future<Boolean> futureResult = completionService.poll(1, TimeUnit.SECONDS);
                                    if (futureResult != null) {
                                        boolean result = futureResult.get().booleanValue();
                                        resultList.add(result);
                                        taskCount = taskCount - 1;
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            // verify all result are ok then commit it otherwise reprocess it.
                            Map<TopicPartition, OffsetAndMetadata> commitOffset =
                                    Collections.singletonMap(
                                            new TopicPartition(record.topic(), record.partition()),
                                            new OffsetAndMetadata(record.offset() + 1));
                            consumer.commitSync(commitOffset);
                            //clear all commit records from list
                            recordList.clear();
                        }
                    }
                    records_processed += records.count();

                    // Print performance stats once per second
                    if ((Math.floor(System.nanoTime() - startTime)/1e9) > last_update) {
                        last_update++;
                        Monitor.print_status(records_processed, poolSize, startTime);
                    }

                }

            }
        } catch (Throwable throwable) {
            System.err.printf("%s", throwable.getStackTrace());
        } finally {
            consumer.close();
            System.out.println("Consumed " + records_processed + " messages from stream.");
            System.out.println("Finished.");
        }

    }



    /* Set the value for configuration parameters.*/
    public static void configureConsumer() {
        Properties props = new Properties();
        props.put("enable.auto.commit","false");
        props.put("group.id", "mapr-workshop");
        props.put("client.id", UUID.randomUUID().toString());
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //  which class to use to deserialize the value of each message
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
    }

}
