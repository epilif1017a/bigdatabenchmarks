package com.epilif.ssbplusstreaming.producers;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class CustomerComplaints extends TimerTask {

    public static String[] reasons = {"failure", "customer unhappy", "product not has announced"};

    private final String topic;
    private final KafkaProducer<String, String> producer;
    private final int interval;
    private final int numRecords;

    public CustomerComplaints(String topic, String kafkaServerUrl, int kafkaServerPort, int interval, int numRecords) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerUrl + ":" + kafkaServerPort);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "SSBPlusProducer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.interval = interval;
        this.numRecords = numRecords;
    }

    public int getInterval() {
        return interval;
    }

    @Override
    public void run() {
        ProducerCallback callback = new ProducerCallback();
        Random rand = new Random();
        String message;
        for (int i = 0; i < this.numRecords; i++) {
            int custKey = rand.nextInt(9000000) + 1;
            int partkey = rand.nextInt(1800000) + 1;
            int suppkey = rand.nextInt(600000) + 1;
            int qtyReturned = rand.nextInt(100) + 1;
            long timestamp = System.currentTimeMillis();
            String reason = reasons[rand.nextInt(3)];
            message = "value";
            ProducerRecord<String, String> data = new ProducerRecord<>(this.topic, message);
            this.producer.send(data, callback);
        }
    }

    private static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.err.printf("Error while producing message to topic %s. Message: %s %n", recordMetadata.topic(), e.getMessage());
            } else {
                System.out.printf("Successfully send message to %s %n", recordMetadata.topic());
            }
        }
    }

    public static void main(String args[]) {
        CustomerComplaints producer = new CustomerComplaints(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        Timer timer = new Timer(true);
        timer.schedule(producer, 0, producer.getInterval());
    }
}
