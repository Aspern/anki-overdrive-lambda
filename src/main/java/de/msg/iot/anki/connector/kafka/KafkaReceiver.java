package de.msg.iot.anki.connector.kafka;

import com.google.gson.Gson;
import com.google.inject.Inject;
import de.msg.iot.anki.connector.Receiver;
import de.msg.iot.anki.data.Data;
import de.msg.iot.anki.data.PositionUpdateMessage;
import de.msg.iot.anki.settings.Settings;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;


public class KafkaReceiver implements Receiver {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Gson serializer = new Gson();

    private Consumer<PositionUpdateMessage> handler;

    @Inject
    public KafkaReceiver(Settings settings) {
        Properties props = new Properties();
        props.put("bootstrap.servers", settings.get("kafka.server"));
        props.put("group.id", settings.get("kafka.group.id"));
        props.put("enable.auto.commit", settings.get("kafka.autocommit"));
        props.put("auto.commit.interval.ms", settings.get("kafka.commit.interval"));
        props.put("session.timeout.ms", settings.get("kafka.session.timeout"));
        props.put("key.deserializer", settings.get("kafka.key.deserializer"));
        props.put("value.deserializer", settings.get("kafka.value.deserializer"));


        this.kafkaConsumer = new KafkaConsumer<>(props);
        this.kafkaConsumer.subscribe(Arrays.asList(
                settings.get("kafka.topic")
        ));
    }

    @Override
    public Receiver onReceive(Consumer consumer) {
        this.handler = consumer;
        return null;
    }

    @Override
    public void run() {
        while (handler != null && !Thread.currentThread().isInterrupted()) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                PositionUpdateMessage message = serializer.fromJson(record.value(), PositionUpdateMessage.class);
                handler.accept(message);
            }
        }

        kafkaConsumer.close();
    }
}
