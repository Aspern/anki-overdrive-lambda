package de.msg.iot.anki.speedlayer;

import java.io.Serializable;

/**
 * Created by msg on 22.03.17.
 */
public class KafkaSQLMessage implements Serializable{
    String message;

    public KafkaSQLMessage(String message){
        this.message = message;
    }
}
