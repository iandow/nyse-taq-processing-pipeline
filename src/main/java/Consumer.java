/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


import java.io.IOException;
import java.util.*;

public class Consumer {

    // Declare a new consumer.
    public static KafkaConsumer consumer;

    private class Trade {
        Date date;
        String exchange;
        String symbol;
        String saleCondition;
        Integer tradeVolume;
        String tradePrice;
        String tradeStopStockIndicator;
        String tradeCorrectionIndicator;
        String tradeSequenceNumber;
        String tradeSource;
        String tradeReportingFacility;
    }

    private static JSONObject parse(String record) {
        if (record.length() < 71) {
            System.err.println("ERROR: Expected line to be at least 71 characters, but got " + record.length());
            return null;
        }

        JSONObject trade_info = new JSONObject();
        trade_info.put("date", record.substring(0, 9));
        trade_info.put("exchange", record.substring(9, 10));
        trade_info.put("symbol root", record.substring(10, 16).trim());
        trade_info.put("symbol suffix", record.substring(16, 26).trim());
        trade_info.put("saleCondition", record.substring(26, 30).trim());
        trade_info.put("tradeVolume", record.substring(30, 39));
        trade_info.put("tradePrice", record.substring(39, 46) + "." + record.substring(46, 50));
        trade_info.put("tradeStopStockIndicator", record.substring(50, 51));
        trade_info.put("tradeCorrectionIndicator", record.substring(51, 53));
        trade_info.put("tradeSequenceNumber", record.substring(53, 69));
        trade_info.put("tradeSource", record.substring(69, 70));
        trade_info.put("tradeReportingFacility", record.substring(70, 71));
        if (record.length() > 71) {
            trade_info.put("sender", record.substring(71, 75));

            JSONArray receiver_list = new JSONArray();
            int i = 0;
            while (record.length() > 75 + i) {
                receiver_list.add(record.substring(75 + i, 79 + i));
                i += 4;
            }
            trade_info.put("receivers", receiver_list);
        }
        return trade_info;

    }

    public static void main(String[] args) throws IOException {
        Runtime runtime = Runtime.getRuntime();
        if (args.length < 1) {
            throw new IllegalArgumentException("You must specify the topic, for example /user/user01/pump:sensor ");
        }

        String topic =  args[0] ;
        System.out.println("Subscribed to : "+ topic);

        configureConsumer();

        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        // Subscribe to the topic.
        consumer.subscribe(topics);

        List<String> buffer = new ArrayList<>();
        long pollTimeOut = 10000;  // milliseconds
        long records_processed = 0L;
        final int minBatchSize = 200;

        // https://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
        // This paradigm is an "at least once delivery" guarantee.
        // TODO: Is it okay for the listener #1 to potentially persist duplicate messages?

        long startTime = System.nanoTime();
        try {
            while (true) {
                // Request unread messages from the topic.
                ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
                if (records.count() == 0) {
                    System.out.println("No messages after " + pollTimeOut/1000 + " second wait.");
                } else {

                    for (ConsumerRecord<String, String> record : records) {
                        buffer.add(record.value());
                    }
                    if (buffer.size() >= minBatchSize) {
                        for (String msg : buffer) {
                            parse(msg);
                            //TODO: insertIntoDb();
                        }
                        consumer.commitSync();
                        buffer.clear();
                    }
                    records_processed += records.count();

                    // Print performance stats
                    long endTime = System.nanoTime();
                    long elapsedTime = endTime - startTime;
                    System.out.format("Throughput = %.0f Kmsgs/sec consumed. Total consumed = %d, Free Memory = %d MB\n", records_processed / ((double) elapsedTime / 1000000000.0) / 1000.0, records_processed, runtime.freeMemory() / (1024 * 1024));

                }

            }
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
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //  which class to use to deserialize the value of each message
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
    }

}
