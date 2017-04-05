package de.msg.iot.anki.speedlayer;

import de.msg.iot.anki.connector.kafka.KafkaProducer;
import de.msg.iot.anki.settings.properties.PropertiesSettings;

/**
 * Created by msg on 17.03.17.
 */
public class SparkController {

    public static void main(String[] args){
        KafkaProducer producer = new KafkaProducer(new PropertiesSettings("settings.properties"));
        int i = 0;
        while(i < 10){
            producer.sendMessage("Hello");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            i++;
        }
        producer.close();
    }
}
