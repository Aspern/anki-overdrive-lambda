package de.msg.iot.anki.speedlayer;

import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.properties.PropertiesSettings;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
import scala.Tuple2;

import java.util.*;

/**
 * Created by msg on 17.03.17.
 */
public class TestReceiver {

    static SparkConf sparkConf;
    static JavaSparkContext sc;
    static JavaStreamingContext jssc;
    static SQLContext sqlContext;

    String jdbcUsername = "root";
    String jdbcPassword = "";
    static String jdbcHostname = "localhost";
    static int jdbcPort = 3306;
    static String jdbcDatabase ="anki";
    //String jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
    static String url="jdbc:mysql://" + jdbcHostname + ":" + jdbcPort + "/" + jdbcDatabase;


    static Function<Tuple2<String, String>, String> mapFunc=new Function<Tuple2<String, String>, String>() {
        @Override
        public String call(Tuple2<String, String> tuple2) {
            return tuple2._2();
        }
    };

    public static void main(String[] args){

        Settings settings = new PropertiesSettings("settings.properties");

        String topic = settings.get("kafka.topic");
        int batchDuration = 4;

        // Create context with a 2 seconds batch interval
        sparkConf = new SparkConf().setAppName("AnkiLambda").setMaster("local[*]");
        sc = new JavaSparkContext(sparkConf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(batchDuration));
        sqlContext = new SQLContext(sc);

        Properties connectionProperties = new java.util.Properties();

        connectionProperties.setProperty("user","root");
        connectionProperties.setProperty("password","");

        jssc.checkpoint(settings.get("kafka.checkpoint"));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("zookeeper.connect", settings.get("zookeeper.url"));
        kafkaParams.put("group.id", settings.get("kafka.group.id"));
        kafkaParams.put("auto.offset.reset", "largest");

        //Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

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
            //System.out.println("received: " + a._2());
            List<Row> list = new ArrayList<>();
            list.add(RowFactory.create(a._2()));
            JavaRDD<Row> rdd = sc.parallelize(list);
            Dataset df = sqlContext.createDataFrame(rdd, schema);
            df.write().mode(SaveMode.Append).jdbc(url, "kafkamsg", connectionProperties);
            return a._2();
        });
        str.print();

        // Start the computation
        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
