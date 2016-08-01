package com.mapr.examples;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class Worker implements Callable<Boolean> {

    ConsumerRecord record;

    public Worker(ConsumerRecord record) {
        this.record = record;
    }

    public Boolean call() {

        Map<String, Object> data = new HashMap<>();
        try {
            // processing steps would we here
            data.put("partition", record.partition());
            data.put("offset", record.offset());
            data.put("value", record.value());

            parse((String)record.value());

            // processing taking one minute
//            Thread.sleep(1*1000);
//            System.out.println("Processing Thread-" + Thread.currentThread().getName() + " data:  " + data);
            return Boolean.TRUE;
        } catch (Exception e) {
            e.printStackTrace();
            return Boolean.FALSE;
        }
    }

    private static JSONObject parse(String record) throws ParseException {
        if (record.length() < 71) {
            throw new ParseException("Expected line to be at least 71 characters, but got " + record.length(), record.length());
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
        if (record.length() >= 74) {
            trade_info.put("sender", record.substring(71, 75));

            JSONArray receiver_list = new JSONArray();
            int i = 0;
            while (record.length() >= 78 + i) {
                receiver_list.add(record.substring(75 + i, 79 + i));
                i += 4;
            }
            trade_info.put("receivers", receiver_list);
        }
        return trade_info;

    }

}