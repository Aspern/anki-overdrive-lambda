package de.msg.iot.anki.speedlayer;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.msg.iot.anki.connector.kafka.KafkaProducer;
import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.properties.PropertiesSettings;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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

    public static String BLUE_VEHICLE_ID = "eb401ef0f82b";
    public static String RED_VEHICLE_ID = "ed0c94216553";

    /*
    * Spark Context variable
    * */
    static SparkConf sparkConf;
    static JavaSparkContext sc;
    static JavaStreamingContext jssc;
    static SQLContext sqlContext;
    static Map<String, Integer> store;
    static KafkaProducer producer;


    public static void handleAntiCollision(String message){

        Float distance = getDistanceFromJson(message, "horizontal");  //get Horizontal distance
        if (distance <= 500)
            brake(message);
        else if (distance > 700)
            speedUp(message);
        //TODO: removed hold speed case because it was only to set light.
    }

    /*
    * Retrieve car speed via id from the hashmap
    * */
    private static int getCarSpeed(String carId){
        return store.get(carId);
    }

    private static void holdSpeed(String message) {
        producer.sendMessage("hold speed");
        int speed = getCarSpeed(getCarIdFromJson(message));
        float messageSpeed = getSpeedFromJson(message);
        if (speed < messageSpeed - 30)
            speedUp(message);
        else
            holdSpeed(message);
    }

    private static void speedUp(String message) {
        final String carId = getCarIdFromJson(message);
        String response = "{" +
                            "\"name\" : \"accelerate\", " +
                            "\"params\" : [" +
                            store.get(carId)+ ", " +   //speed //TODO: using static speed for accel.
                            "0.08" +     //acceleration
                            "]" +
                          "}";
        producer.sendMessage(response);
        producer.sendMessage(response, getCarIdFromJson(message));
    }

    private static void driveNormal(String message) {

        String response = "{" +
                "\"name\" : \"set-speed\", " +
                "\"params\" : [" +
                getSpeedFromJson(message) +   //speed
                "," +
                "250" +     //acceleration
                "]" +
                "}";
        producer.sendMessage(response);
        producer.sendMessage(response, getCarIdFromJson(message));
    }

    private static void brake(String message) {
        String response = "{" +
                            "\"name\" : \"brake\", " +
                            "\"params\" : [" +
                            "0.15" +
                            "]" +
                          "}";
        producer.sendMessage(response);
        producer.sendMessage(response, getCarIdFromJson(message));

        //send speed message via kafka
        //record.vehicle.setSpeed(message.speed - 50, 200);
    }

    private static String getCarIdFromJson(String json){

        JsonElement jelement = new JsonParser().parse(json);
        JsonObject jobject = jelement.getAsJsonObject();
        if(!jobject.has("vehicleId")) return null;
        String carId = jobject.get("vehicleId").toString().replaceAll("^\"|\"$", "");
        return carId;
    }

    private static float getSpeedFromJson(String json){

        JsonElement jelement = new JsonParser().parse(json);
        JsonObject jobject = jelement.getAsJsonObject();
        String speed = jobject.get("speed").toString();
        return Float.parseFloat(speed);
    }

    private static Float getDistanceFromJson(String json, String type){
        JsonElement jelement = new JsonParser().parse(json);
        JsonObject jobject = jelement.getAsJsonObject();

        if(!(Integer.parseInt(jobject.get("messageId").toString()) == 39)) return null;

        JsonArray jarray = jobject.getAsJsonArray("distances");
        if(jarray.size() == 0) return null;
        jobject = jarray.get(0).getAsJsonObject();
        String result = jobject.get(type).toString();
        return result.equals("null") ? null : Float.parseFloat(result);
    }

    public static void main(String[] args){

        Settings settings = new PropertiesSettings("settings.properties");

        producer = new KafkaProducer(settings, "test");

        store = new HashMap<String, Integer>() {{
            put(BLUE_VEHICLE_ID, 400); //blue car inner lane @beginning
            put(RED_VEHICLE_ID, 600); //red car outer lane @beginning
        }};

        String topic = settings.get("kafka.topic");

        // Batch duration for the streaming window
        int batchDuration = 500; //TODO: Changed batch window

        // Url to save data to mysql db
        String url="jdbc:mysql://" + settings.get("mysql.url") + ":" + settings.get("mysql.port") + "/" + settings.get("mysql.database");

        // Define the configuration for spark context
        sparkConf = new SparkConf().setAppName("AnkiLambda").setMaster("local[*]");

        // Initialize the spark context
        sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        // This context is for receiving real-time stream
        jssc = new JavaStreamingContext(sc, Durations.milliseconds(batchDuration)); //TODO: changed unit.

        // This context is used tto save messages to mysql db
        sqlContext = new SQLContext(sc);

        // Define the properties for mysql db
        Properties connectionProperties = new java.util.Properties();
        connectionProperties.setProperty("user", settings.get("mysql.user"));
        connectionProperties.setProperty("password", settings.get("mysql.password"));

        // Initialize the checkpoint for spark
        jssc.checkpoint("/home/aweber/testfolder");

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


        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

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

            String carId = getCarIdFromJson(a._2().toString());

            //String carId = a.toString().split(",")[1];
            //TODO: using static speeds.
//            if(!store.containsKey(carId) && carId != null){
//                store.put(carId, 200);
//            }

            //Prepare data to store in database
            List<Row> list = new ArrayList<>();
            list.add(RowFactory.create(a._2()));
            JavaRDD<Row> rdd = sc.parallelize(list);
            Dataset df = sqlContext.createDataFrame(rdd, schema);
            //df.write().mode(SaveMode.Append).jdbc(url, "kafkamsg", connectionProperties);
            return a._2().toString();
        });

        // Print the received and transformed stream : key and value
        //kafkaStream.print();

        //print the value from stream
        //str.print();

        JavaDStream<String> car1 = str.filter(x -> {
            String carId = getCarIdFromJson(x.toString());
            if(carId != null && carId.equals(BLUE_VEHICLE_ID)){
                return Boolean.TRUE;
            }
            else {
                return Boolean.FALSE;
            }
        });

        //car1.print();

        JavaDStream<String> car2 = str.filter(x -> {
            String carId = getCarIdFromJson(x.toString());
            if(carId != null && carId.equals(RED_VEHICLE_ID)){
                return Boolean.TRUE;
            }
            else {
                return Boolean.FALSE;
            }
        });

        //car2.print();

        JavaDStream<String> antiCollisionCar1 = str.map(x -> {
            Float delta = getDistanceFromJson(x.toString(), "delta");
            Float verticalDistance = getDistanceFromJson(x.toString(), "vertical");

            if(delta == null || verticalDistance == null) return x;

            if(delta < 0 && verticalDistance <= 34){
                handleAntiCollision(x);
            } else if(getSpeedFromJson(x.toString()) < store.get(getCarIdFromJson(x.toString())) - 30) //TODO: speed up again on other lane
                speedUp(x.toString());
            return x;
        });


        antiCollisionCar1.print();

        // Start the computation
        jssc.start();

        // Don't stop the execution until user explicitly stops or we can pass the duration for the execution
        try {
            jssc.awaitTermination();
            producer.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
