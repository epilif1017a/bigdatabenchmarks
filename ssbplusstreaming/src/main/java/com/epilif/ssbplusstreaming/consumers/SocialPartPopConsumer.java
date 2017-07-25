package com.epilif.ssbplusstreaming.consumers;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;

public class SocialPartPopConsumer {

    public static void main(String[] args) {

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "node5.dsi.uminho.pt:6667");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark.events");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT");

        Collection<String> topics = Arrays.asList("carlos");
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

        SparkConf conf = new SparkConf()
                .setAppName("StreamingCPEWorkload")
                .set("spark.cassandra.connection.host", "node2.dsi.uminho.pt,node11.dsi.uminho.pt")
                .set("spark.cassandra.auth.username", "presto")
                .set("spark.cassandra.auth.password", "prestoCassandra");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaInputDStream<ConsumerRecord<Integer, String>> stream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        stream.foreachRDD((JavaRDD<ConsumerRecord<Integer, String>> rdd) -> {
            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            offsetRanges.set(offsets);
        });

        JavaDStream<SocialPartPopRow> transformedStream = stream.map((ConsumerRecord<Integer, String> event) -> {
            String[] fields = event.value().split("\";\"");
            Matcher m = Pattern.compile("product=(.*)&redirected=(.*)").matcher(fields[1]);
            m.find();
            return new SocialPartPopRow(fields[0], m.group(1), Boolean.parseBoolean(m.group(2)));
        });

        transformedStream.print();

        transformedStream.foreachRDD((JavaRDD<SocialPartPopRow> rdd) -> {
            javaFunctions(rdd).writerBuilder("carlos", "dummy_sales", mapToRow(SocialPartPopRow.class)).saveToCassandra();
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges.get());
        });

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException ex) {
            System.err.printf("The application '%s' has stopped! ", conf.getAppId());
        }
    }

}
