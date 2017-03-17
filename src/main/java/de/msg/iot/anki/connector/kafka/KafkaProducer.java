package de.msg.iot.anki.connector.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by msg on 17.03.17.
 */
public class KafkaProducer {

    private String topic;
    private Producer producer;
    private Properties props;

    public KafkaProducer(String topic){
        this.topic = topic;

        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "WebLogProducer");  // For figuring out exception

        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
    }

    public void sendMessage(String message){
        ProducerRecord<String, String> rec = new ProducerRecord<>(topic, message);
        producer.send(rec);
    }

    public void close(){
        producer.close();
    }
}
