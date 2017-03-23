package de.msg.iot.anki.speedlayer;

import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.properties.PropertiesSettings;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;

/**
 * Created by msg on 17.03.17.
 */
public class TestReceiver {

    /*
    * Spark Context variable
    * */
    static SparkConf sparkConf;
    static JavaSparkContext sc;
    static JavaStreamingContext jssc;
    static SQLContext sqlContext;

    public static void main(String[] args){

        Settings settings = new PropertiesSettings("settings.properties");

        String topic = settings.get("kafka.topic");

        // Batch duration for the streaming window
        int batchDuration = 4;

        // Url to save data to mysql db
        String url="jdbc:mysql://" + settings.get("mysql.url") + ":" + settings.get("mysql.port") + "/" + settings.get("mysql.database");

        // Define the configuration for spark context
        sparkConf = new SparkConf().setAppName("AnkiLambda").setMaster("local[*]");

        // Initialize the spark context
        sc = new JavaSparkContext(sparkConf);

        // This context is for receiving real-time stream
        jssc = new JavaStreamingContext(sc, Durations.seconds(batchDuration));

        // This context is used tto save messages to mysql db
        sqlContext = new SQLContext(sc);

        // Define the properties for mysql db
        Properties connectionProperties = new java.util.Properties();
        connectionProperties.setProperty("user", settings.get("mysql.user"));
        connectionProperties.setProperty("password", settings.get("mysql.password"));

        // Initialize the checkpoint for spark
        jssc.checkpoint(settings.get("kafka.checkpoint"));

        // Kafka receiver properties
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("zookeeper.connect", settings.get("zookeeper.url"));
        kafkaParams.put("group.id", settings.get("kafka.group.id"));
        kafkaParams.put("auto.offset.reset", "largest");

        //Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        // Add topic to the Hashmap. We can add multiple topics here
        Map<String,Integer> topicMap=new HashMap<>();
        topicMap.put(topic,1);

        /*
        * Schema for saving messages into mysql database
        * */
        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField("message", DataTypes.StringType, false)});

        /*
        * Create kafka stream to receive messages
        * */
        JavaPairReceiverInputDStream<String, String> kafkaStream=KafkaUtils.createStream(
                        jssc,
                        String.class,
                        String.class,
                        kafka.serializer.StringDecoder.class,
                        kafka.serializer.StringDecoder.class,
                        kafkaParams,
                        topicMap,
                        StorageLevel.MEMORY_AND_DISK()
        );

        /*
        * Get only the value from stream and meanwhile save message in the mysql aswell
        * */
        JavaDStream<String> str = kafkaStream.map(a -> {
            List<Row> list = new ArrayList<>();
            list.add(RowFactory.create(a._2()));
            JavaRDD<Row> rdd = sc.parallelize(list);
            Dataset df = sqlContext.createDataFrame(rdd, schema);
            df.write().mode(SaveMode.Append).jdbc(url, "kafkamsg", connectionProperties);
            return a._2();
        });

        // Print the received and transformed stream
        str.print();

        // Start the computation
        jssc.start();

        // Don't stop the execution until user explicitly stops or we can pass the duration for the execution
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
