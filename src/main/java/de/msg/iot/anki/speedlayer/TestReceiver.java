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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by msg on 17.03.17.
 */
public class TestReceiver {

    public static String RED_VEHICLE_ID = "d5255cd93a2b";
    public static String BLUE_VEHICLE_ID = "ef4ace474907";

    /*
    * Spark Context variable
    * */
    static SparkConf sparkConf;
    static JavaSparkContext sc;
    static JavaStreamingContext jssc;
    static SQLContext sqlContext;
    static Map<String, Integer> store;
    static KafkaProducer producer;
    static int roundNumber = 0;
    static int batteryLevel = 0;
    static  Boolean is34 = false;
    static PrintWriter pw;

    public static void handleAntiCollision(String message){
        System.out.println("in anti collo method");
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
        System.out.println("speeding up");
        producer.sendMessage(response, "test");
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
        System.out.println("driving normal");
        producer.sendMessage(response, "test");
        producer.sendMessage(response, getCarIdFromJson(message));
    }

    private static void brake(String message) {
        String response = "{" +
                            "\"name\" : \"brake\", " +
                            "\"params\" : [" +
                            "0.15" +
                            "]" +
                          "}";
        System.out.println("applying brake");
        producer.sendMessage(response, "test");
        producer.sendMessage(response, getCarIdFromJson(message));
    }

    private static String getCarIdFromJson(String json){

        JsonElement jelement = new JsonParser().parse(json);
        JsonObject jobject = jelement.getAsJsonObject();
        if(!jobject.has("vehicleId")) return null;
        String carId = jobject.get("vehicleId").toString().replaceAll("^\"|\"$", "");
        return carId;
    }

    private static Integer getMessageIdFromJson(String json){

        JsonElement jelement = new JsonParser().parse(json);
        JsonObject jobject = jelement.getAsJsonObject();
        if(!jobject.has("messageId")) return null;
        int messageId = Integer.parseInt(jobject.get("messageId").toString());
        return messageId;
    }

    private static float getSpeedFromJson(String json){

        JsonElement jelement = new JsonParser().parse(json);
        JsonObject jobject = jelement.getAsJsonObject();
        String speed = jobject.get("speed").toString();
        return Float.parseFloat(speed);
    }

    private static int getBatteryLevelFromJson(String json){

        JsonElement jelement = new JsonParser().parse(json);
        JsonObject jobject = jelement.getAsJsonObject();
        String level = jobject.get("batteryLevel").toString();
        return Integer.parseInt(level);
    }

    private static int getPieceFromJson(String json){

        JsonElement jelement = new JsonParser().parse(json);
        JsonObject jobject = jelement.getAsJsonObject();
        String piece = jobject.get("piece").toString();
        return Integer.parseInt(piece);
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
        int batchDuration = 800; //TODO: Changed batch window


        // Define the configuration for spark context
        sparkConf = new SparkConf().setAppName("AnkiLambda").setMaster("local[*]");

        // Initialize the spark context
        sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        // This context is for receiving real-time stream
        jssc = new JavaStreamingContext(sc, Durations.milliseconds(batchDuration)); //TODO: changed unit.

        // This context is used tto save messages to mysql db
        sqlContext = new SQLContext(sc);

        Logger.getLogger("all").setLevel(Level.OFF);

        // Initialize the checkpoint for spark
        //jssc.checkpoint(settings.get("kafka.checkpoint"));
        jssc.checkpoint("/home/msg/Documents/tmp");

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


        JavaDStream<String> str = kafkaStream.map(a -> a._2().toString());

        JavaDStream<String> batteryMessagesStream = str.filter(a -> {
            Integer messageId = getMessageIdFromJson(a);
            if(messageId != null && messageId == 27){
                return Boolean.TRUE;
            }
            else {
                return Boolean.FALSE;
            }
        });


        JavaDStream<String> bstr = batteryMessagesStream.map(a -> {
            batteryLevel = getBatteryLevelFromJson(a);
            System.out.println("Battery is : " + batteryLevel);
            return a;
        });


        JavaDStream<String> speedMessagesStream = str.filter(a -> {
            Integer messageId = getMessageIdFromJson(a);
            if(messageId != null && messageId == 39){
                if(getPieceFromJson(a) == 34) {
                    is34 = !is34;
                    if(is34){
                        roundNumber++;
                        //return Boolean.TRUE;
                    }
                    return Boolean.TRUE;
                }
                return Boolean.FALSE;
            }
            else {
                return Boolean.FALSE;
            }
        });
        *//*

        pw = null;
        try {
            pw = new PrintWriter(new File("/home/msg/Documents/BatteryLevel_500_skull.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        StringBuilder sb = new StringBuilder();

        JavaDStream<String> str1 = str.map(a -> {

            if(getPieceFromJson(a) == 34) {
                is34 = !is34;
                if (is34) {
                    roundNumber++;
                }
            }

            sb.append(RED_VEHICLE_ID);
            sb.append(',');
            sb.append(roundNumber);
            sb.append(',');
            sb.append(getPieceFromJson(a));
            sb.append(',');
            sb.append(getBatteryLevelFromJson(a));
            sb.append(',');
            sb.append(getSpeedFromJson(a));
            sb.append('\n');

            pw.write(sb.toString());

            return "Round: " + roundNumber + ", Battery: " + batteryLevel ;
        });

        str1.print();

        JavaDStream<String> antiCollision = speedMessagesStream.map(x -> {
            Float delta = getDistanceFromJson(x.toString(), "delta");
            Float verticalDistance = getDistanceFromJson(x.toString(), "vertical");

            if(delta == null || verticalDistance == null) {
                System.out.println("Delta or vertical distance is null -- ");
                System.out.println(x.toString());
                return x;
            }

            if(delta < 0 && verticalDistance <= 34){
                handleAntiCollision(x);
            } else if(getSpeedFromJson(x.toString()) < store.get(getCarIdFromJson(x.toString())) - 30) //TODO: speed up again on other lane
                speedUp(x.toString());
            return x;
        });


        jssc.start();

        // Don't stop the execution until user explicitly stops or we can pass the duration for the execution
        try {
            jssc.awaitTermination();
            producer.close();

            pw.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}