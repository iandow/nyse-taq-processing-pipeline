package com.mapr.examples;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Inbox;
import scala.concurrent.duration.Duration;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.io.Serializable;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ParseAndPersist {
    public static int count = 0;
    public static class Status implements Serializable {}
    public static class ParseMe implements Serializable {
        public final String rawtext;
        public ParseMe(String rawtext) {
            this.rawtext = rawtext;
        }
    }

    public static class Parser extends UntypedActor {
        JSONObject json = new JSONObject();

        public void onReceive(Object message) {
            if (message instanceof Status) {
                System.out.println("count = " + count);
            }
            if (message instanceof ParseMe)
                try {
                    json = doParse(((ParseMe) message).rawtext);
                    count++;
                } catch (ParseException e) {
                    System.err.printf("%s", e.getStackTrace());
                }
            else unhandled(message);
        }

        private static JSONObject doParse(String rawtext) throws ParseException {
            if (rawtext.length() < 71) {
                throw new ParseException("Expected line to be at least 71 characters, but got " + rawtext.length(), rawtext.length());
            }

            JSONObject json = new JSONObject();
            json.put("date", rawtext.substring(0, 9));
            json.put("exchange", rawtext.substring(9, 10));
            json.put("symbol root", rawtext.substring(10, 16).trim());
            json.put("symbol suffix", rawtext.substring(16, 26).trim());
            json.put("saleCondition", rawtext.substring(26, 30).trim());
            json.put("tradeVolume", rawtext.substring(30, 39));
            json.put("tradePrice", rawtext.substring(39, 46) + "." + rawtext.substring(46, 50));
            json.put("tradeStopStockIndicator", rawtext.substring(50, 51));
            json.put("tradeCorrectionIndicator", rawtext.substring(51, 53));
            json.put("tradeSequenceNumber", rawtext.substring(53, 69));
            json.put("tradeSource", rawtext.substring(69, 70));
            json.put("tradeReportingFacility", rawtext.substring(70, 71));
            if (rawtext.length() >= 74) {
                json.put("sender", rawtext.substring(71, 75));

                JSONArray receiver_list = new JSONArray();
                int i = 0;
                while (rawtext.length() >= 78 + i) {
                    receiver_list.add(rawtext.substring(75 + i, 79 + i));
                    i += 4;
                }
                json.put("receivers", receiver_list);
            }
            return json;
        }
    }

}
