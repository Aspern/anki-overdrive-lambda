package de.msg.iot.anki.speedlayer;

import de.msg.iot.anki.connector.kafka.KafkaProducer;
import de.msg.iot.anki.settings.properties.PropertiesSettings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

/**
 * Created by msg on 04.04.17.
 */
public class StreamProducer {

    public static void main(String[] args){

        KafkaProducer producer = new KafkaProducer(new PropertiesSettings("settings.properties"));

        String csvFile = "/home/msg/Desktop/car_data/position-update.csv";
        String line = "";
        String cvsSplitBy = ",";

        Random rnd = new Random();

        Long timestamp = System.currentTimeMillis();
        Long adjustedTimestamp = timestamp;

        // introduce a bit of randomness to time increments for demo purposes
        long incrementTimeEvery = rnd.nextInt(Math.min(1000, 100) - 1) + 1;

        int i = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * 400);  //400   is time multiplier
                timestamp = System.currentTimeMillis(); // move all this to a function

                // use comma as separator
                String[] record = line.split(cvsSplitBy);

                producer.sendMessage(line);

                //System.out.println("Anki > [timestamp= " + record[0] + " , car_id=" + record[1] + "]");

                if (i % incrementTimeEvery == 0) {
                    System.out.println("Sent " + i +" messages!");
                    int sleeping = rnd.nextInt(1500);
                    System.out.println("Sleeping for " + sleeping + " ms");
                    try {
                        Thread.sleep(sleeping);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                i++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        producer.close();
    }

}
