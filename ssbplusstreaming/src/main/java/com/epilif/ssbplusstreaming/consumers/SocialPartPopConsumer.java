package com.epilif.ssbplusstreaming.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

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

        Collection<String> topics = Arrays.asList("social_part_popularity");
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

        SparkConf conf = new SparkConf()
                .setAppName("StreamingCPEWorkload")
                .set("spark.cassandra.connection.host", "node2.dsi.uminho.pt,node11.dsi.uminho.pt")
                .set("spark.cassandra.auth.username", "presto")
                .set("spark.cassandra.auth.password", "prestoCassandra");
                //.set("spark.cassandra.connection.connections_per_executor_max", "1")
                //.set("spark.cassandra.connection.keep_alive_ms", "1000")
                //.set("spark.cassandra.output.batch.grouping.key", "replica_set")
                //.set("spark.cassandra.output.batch.size.rows", "50")
                //.set("spark.cassandra.output.concurrent.writes", "2");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        SparkSession spark = new SparkSession(JavaSparkContext.toSparkContext(jssc.sparkContext()));

        JavaPairRDD<Integer, String> partCategories = jssc.sparkContext().textFile(args[0]).
                mapToPair(s -> {
                    String[] split = s.split("\\|");
                    return new Tuple2(Integer.parseInt(split[0]), split[3]);
                });

        Broadcast<JavaPairRDD<Integer, String>> broadcastedPartCats = jssc.sparkContext().broadcast(partCategories);

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        stream.foreachRDD((JavaRDD<ConsumerRecord<String, String>> rdd) -> {
            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            offsetRanges.set(offsets);
        });

        JavaDStream<SocialPartPopRow> transformedStream = stream.map((ConsumerRecord<String, String> event) -> {
            String[] fields = event.value().split("\";\"");
            return new SocialPartPopRow(
                    Integer.parseInt(fields[0]),
                    Integer.parseInt(fields[1]),
                    fields[2],
                    fields[3],
                    fields[4],
                    Integer.parseInt(fields[5])
            );
        });

        transformedStream.foreachRDD((JavaRDD<SocialPartPopRow> rdd) -> {
            javaFunctions(rdd).writerBuilder("ssbplus", "social_part_popularity", mapToRow(SocialPartPopRow.class)).saveToCassandra();
            Dataset ds = spark.createDataFrame(rdd, SocialPartPopRow.class);
            ds.select("partkey",
                    "datekey",
                    "timekey",
                    "country",
                    "gender",
                    "sentiment")
                    .write()
                    .mode("append")
                    .orc("/apps/hive/warehouse/ssbplus300.db/social_part_popularity");
        });

        JavaDStream<SocialPartPopFlatRow> joinedStream = transformedStream.transform((JavaRDD<SocialPartPopRow> rdd) ->
                rdd.mapToPair(row -> new Tuple2<>(row.getPartkey(), row))
                        .join(broadcastedPartCats.value())
                        .map(tuple -> new SocialPartPopFlatRow(
                                tuple._1(),
                                tuple._2()._2(),
                                tuple._2()._1().getDatekey(),
                                Integer.parseInt(tuple._2()._1().getTimekey().substring(0, 2)),
                                Integer.parseInt(tuple._2()._1().getTimekey().substring(2)),
                                tuple._2()._1().getCountry(),
                                tuple._2()._1().getGender(),
                                tuple._2()._1().getSentiment()
                        )));

        joinedStream.foreachRDD((JavaRDD<SocialPartPopFlatRow> rdd) -> {
            javaFunctions(rdd).writerBuilder("ssbplus", "social_part_popularity_flat", mapToRow(SocialPartPopFlatRow.class)).saveToCassandra();
            Dataset ds = spark.createDataFrame(rdd, SocialPartPopFlatRow.class);
            ds.select("partkey",
                    "partcategory",
                    "datekey",
                    "hour",
                    "minutes",
                    "country",
                    "gender",
                    "sentiment")
                    .write()
                    .mode("append")
                    .orc("/apps/hive/warehouse/ssbplus300.db/social_part_popularity_flat");
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges.get());
        });

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException ex) {
            broadcastedPartCats.unpersist();
            broadcastedPartCats.destroy();
            System.err.printf("The application '%s' has stopped! ", conf.getAppId());
        }
    }

}