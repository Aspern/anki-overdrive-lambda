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
        private final boolean interrupt;

        public Info(String name, boolean interrupt) {
            this.name = name;
            this.interrupt = interrupt;
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

    public void collisionScenario(boolean interrupt) {
        sendMessage("collision", interrupt);
    }

    public void antiCollisionScenario(boolean interrupt) {
        sendMessage("anti-collision", interrupt);
    }

    public void maxSpeedScenario(boolean interrupt) {
        sendMessage("max-speed", interrupt);
    }

    private void sendMessage(final String name, final boolean interrupt) {
        this.producer.send(new ProducerRecord<>(
                "scenario",
                "Message-" + counter.getAndIncrement(),
                serializer.toJson(new Info(name, interrupt))
        ));
    }

}
