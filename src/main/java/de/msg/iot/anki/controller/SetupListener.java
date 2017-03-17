package de.msg.iot.anki.controller;


import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Injector;
import de.msg.iot.anki.batchlayer.masterdata.elastic.ElasticMasterDataSet;
import de.msg.iot.anki.settings.Settings;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SetupListener implements Runnable {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Gson serializer = new Gson();
    private final Map<String, AnkiOverdriveSetup> setups = new HashMap<>();
    private final Injector injector;
    private final Logger logger = Logger.getLogger(SetupListener.class);
    private volatile boolean running = false;
    private Thread thread;

    @Inject
    public SetupListener(Settings settings, Injector injector) {
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
                "setup"
        ));
        this.injector = injector;
    }

    @Override
    public void run() {
        logger.info("Starting SetupListener...");
        while (!Thread.currentThread().isInterrupted()) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                AnkiOverdriveSetupMessage message = serializer.fromJson(record.value(), AnkiOverdriveSetupMessage.class);
                final String name = message.getName();
                if (message.isOnline() && !setups.containsKey(name)) {
                    AnkiOverdriveSetup setup = injector.getInstance(AnkiOverdriveSetup.class);
                    setup.setup(message);
                    setups.put(name, setup);
                    logger.info("Added setup [" + name + "].");
                } else if (message.isOnline() && setups.containsKey(name)) {
                    setups.remove(name);
                    logger.info("Removed setup [" + name + "].");
                }
            }
        }
    }

    public AnkiOverdriveSetup getSetup(String name) {
        return this.setups.get(name);
    }

    public void start() {
        if (running)
            return;

        thread = new Thread(this);
        thread.start();
        running = true;
    }

    public void stop() {
        if (!running)
            return;

        thread.interrupt();
        running = false;
    }
}
