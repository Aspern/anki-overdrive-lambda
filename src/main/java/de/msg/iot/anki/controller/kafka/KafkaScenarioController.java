package de.msg.iot.anki.controller.kafka;


import com.google.gson.Gson;
import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.properties.PropertiesSettings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaScenarioController {

    private final Gson serializer = new Gson();
    private final KafkaProducer<String, String> producer;
    private final AtomicInteger counter = new AtomicInteger();

    private String uuid;

    private static class Info {
        private final String name;

        public Info(String name) {
            this.name = name;
        }
    }

    public KafkaScenarioController() {
        final Settings settings = new PropertiesSettings("settings.properties");
        this.uuid = uuid;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", settings.get("kafka.server"));
        properties.put("client.id", UUID.randomUUID().toString());
        properties.put("key.serializer", settings.get("kafka.key.serializer"));
        properties.put("value.serializer", settings.get("kafka.value.serializer"));

        this.producer = new KafkaProducer<>(properties);
    }

    public void startCollisionScenario() {
        sendMessage("collision");
    }

    public void startAntiCollisionScenario() {
        sendMessage("anti-collision");
    }

    private void sendMessage(final String name) {
        this.producer.send(new ProducerRecord<>(
                "scenario",
                "Message-" + counter.getAndIncrement(),
                serializer.toJson(new Info(name))
        ));
    }

}
