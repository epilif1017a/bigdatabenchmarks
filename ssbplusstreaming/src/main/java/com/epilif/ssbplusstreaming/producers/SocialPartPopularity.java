package com.epilif.ssbplusstreaming.producers;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class SocialPartPopularity extends TimerTask {

    private static String[] countries = {"Portugal", "Spain", "United Kingdom", "Germany", "Italy", "France"};
    private static String[] genders = {"Male", "Female"};

    private final String topic;
    private final KafkaProducer<String, String> producer;
    private final int intervalMs;
    private final int numRecords;

    public SocialPartPopularity(String topic, String kafkaServerUrl, int kafkaServerPort, int intervalMs, int numRecords) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerUrl + ":" + kafkaServerPort);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "socialpartpopularity");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.intervalMs = intervalMs;
        this.numRecords = numRecords;
    }

    public int getIntervalMs() {
        return this.intervalMs;
    }

    @Override
    public void run() {
        ProducerCallback callback = new ProducerCallback();
        Random rand = new Random();
        for (int i = 0; i < this.numRecords; i++) {
            int partkey = rand.nextInt(1800000) + 1;
            int datekey = Integer.parseInt(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")));
            String timekey = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HHmm"));
            String country = countries[rand.nextInt(6)];
            String gender = genders[rand.nextInt(2)];
            int sentiment = rand.nextInt(5) + 1;
            ProducerRecord<String, String> data = new ProducerRecord<>(this.topic,
                    partkey + "\";\"" +
                            datekey + "\";\"" +
                            timekey + "\";\"" +
                            country + "\";\"" +
                            gender + "\";\"" +
                            sentiment
            );
            this.producer.send(data, callback);
        }
    }

    private static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.err.printf("Error while producing message to topic %s. Message: %s %n", recordMetadata.topic(), e.getMessage());
            }
        }
    }

    public static void main(String args[]) {
        SocialPartPopularity producer = new SocialPartPopularity(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        Timer timer = new Timer();
        timer.schedule(producer, 0, producer.getIntervalMs());
    }
}