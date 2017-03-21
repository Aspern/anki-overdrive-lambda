package de.msg.iot.anki.speedlayer;

import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.properties.PropertiesSettings;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;

/**
 * Created by msg on 17.03.17.
 */
public class TestReceiver {
    public static void main(String[] args){

        Settings settings = new PropertiesSettings("settings.properties");

        String topic = settings.get("kafka.topic");
        int batchDuration = 4;

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("AnkiLambda").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDuration));

        jssc.checkpoint(settings.get("kafka.checkpoint"));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("zookeeper.connect", settings.get("zookeeper.url"));
        kafkaParams.put("group.id", settings.get("kafka.group.id"));
        kafkaParams.put("auto.offset.reset", "largest");

        //Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        Map<String,Integer> topicMap=new HashMap<>();
        topicMap.put(topic,1);

        JavaPairReceiverInputDStream<String,String> kafkaStream=KafkaUtils.createStream(jssc,String.class,String.class,kafka.serializer.StringDecoder.class,kafka.serializer.StringDecoder.class,kafkaParams,topicMap, StorageLevel.MEMORY_AND_DISK());

        kafkaStream.print();

        // Start the computation
        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
