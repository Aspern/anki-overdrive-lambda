package de.msg.iot.anki.speedlayer;

import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.properties.PropertiesSettings;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
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
    static Map<String, Integer> store;


    public static void handleAntiCollision(String message){
        float distance = Float.parseFloat(message.split(",")[14]);  //get Horizontal distance
        if (distance <= 500)
            brake(message);
        else if (distance > 700)
            driveNormal(message);
        else
            holdSpeed(message);
    }

    /*
    * Retrieve car speed via id from the hashmap
    * */
    private static int getCarSpeed(String carId){
        return store.get(carId);
    }

    private static void holdSpeed(String message) {
        System.out.println("Holding Speed : " + message.split(",")[3]);

        int speed = getCarSpeed(message.split(",")[1]);
        int messageSpeed = Integer.parseInt(message.split(",")[3]);
        if (speed < messageSpeed - 30)
            speedUp(message);
        else
            holdSpeed(message);
    }

    private static void speedUp(String message) {
        System.out.println("Speed up : " + message.split(",")[3]);

        int speed = getCarSpeed(message.split(",")[1]);

        //Speed up car, send message via kafka
        //record.vehicle.setSpeed(speed, 50);
    }

    private static void driveNormal(String message) {
        System.out.println("Driving Normal : " + message.split(",")[3]);
    }

    private static void brake(String message) {
        System.out.println("Applying Brake : " + message.split(",")[3]);

        int speed = getCarSpeed(message.split(",")[1]);

        //send speed message via kafka
        //record.vehicle.setSpeed(message.speed - 50, 200);
    }


    public static void main(String[] args){

        Settings settings = new PropertiesSettings("settings.properties");

        store = new HashMap<>();

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
            String carId = a.toString().split(",")[1];
            if(!store.containsKey(carId)){
                store.put(carId, 0);
            }

            //Prepare data to store in database
            List<Row> list = new ArrayList<>();
            list.add(RowFactory.create(a._2()));
            JavaRDD<Row> rdd = sc.parallelize(list);
            Dataset df = sqlContext.createDataFrame(rdd, schema);
            //df.write().mode(SaveMode.Append).jdbc(url, "kafkamsg", connectionProperties);
            return a._2();
        });

        // Print the received and transformed stream : key and value
        //kafkaStream.print();

        //print the value from stream
        //str.print();

        JavaDStream<String> car1 = str.filter(x -> {
            String id = x.split(",")[1];
            if(id.equals("eb401ef0f82b")){
                return Boolean.TRUE;
            }
            else {
                return Boolean.FALSE;
            }
        });

        //car1.print();

        JavaDStream<String> car2 = str.filter(x -> {
            String id = x.split(",")[1];
            if(id.equals("ed0c94216553")){
                return Boolean.TRUE;
            }
            else {
                return Boolean.FALSE;
            }
        });

        //car2.print();

        JavaDStream<String> antiCollisionCar1 = car1.map(x -> {
            float delta = Float.parseFloat(x.split(",")[16]);
            float verticalDistance = Float.parseFloat(x.split(",")[15]);
            if(delta < 0 && verticalDistance <= 34){
                handleAntiCollision(x);
            }
            return x;
        });

        antiCollisionCar1.print();

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
