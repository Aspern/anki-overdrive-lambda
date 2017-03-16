package de.msg.iot.anki.controller.kafka;

import com.google.gson.Gson;
import de.msg.iot.anki.controller.VehicleController;
import de.msg.iot.anki.settings.Settings;
import de.msg.iot.anki.settings.properties.PropertiesSettings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;


public class KafkaVehicleController implements VehicleController {

    private final Gson serializer = new Gson();
    private final KafkaProducer<String, String> producer;
    private final AtomicInteger counter = new AtomicInteger();

    private String uuid;

    public KafkaVehicleController(String uuid) {
        final Settings settings = new PropertiesSettings("settings.properties");
        this.uuid = uuid;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", settings.get("kafka.server"));
        properties.put("client.id", UUID.randomUUID().toString());
        properties.put("key.serializer", settings.get("kafka.key.serializer"));
        properties.put("value.serializer", settings.get("kafka.value.serializer"));

        this.producer = new KafkaProducer<>(properties);
    }

    @Override
    public void connect() {
        this.sendMessage(new Command(
                "connect",
                new Number[0]
        ));
    }

    @Override
    public void disconnect() {
        this.sendMessage(new Command(
                "disconnect",
                new Number[0]
        ));
    }

    @Override
    public void setSpeed(int speed, int acceleration) {
        this.sendMessage(new Command(
                "set-speed",
                new Number[]{speed, acceleration}
        ));
    }

    @Override
    public void setOffset(float offset) {
        this.sendMessage(new Command(
                "set-offset",
                new Number[]{offset}
        ));
    }

    @Override
    public void changeLane(float offset) {
        this.sendMessage(new Command(
                "change-lane",
                new Number[]{offset}
        ));
    }

    private void sendMessage(Command command) {
        this.producer.send(new ProducerRecord<>(
                uuid,
                "Message-" + counter.getAndIncrement(),
                serializer.toJson(command)
        ));
    }
}