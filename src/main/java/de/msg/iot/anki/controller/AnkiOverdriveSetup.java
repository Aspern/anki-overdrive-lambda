package de.msg.iot.anki.controller;


import com.google.inject.Inject;
import com.google.inject.Injector;
import de.msg.iot.anki.controller.kafka.KafkaVehicleController;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class AnkiOverdriveSetup {

    private final Map<String, KafkaVehicleController> vehicles = new HashMap<>();
    private final Injector injector;

    @Inject
    public AnkiOverdriveSetup(Injector injector) {
        this.injector = injector;
    }

    public void setup(AnkiOverdriveSetupMessage message) {
        message.getVehicles().forEach(uuid -> {
            KafkaVehicleController controller = this.injector.getInstance(KafkaVehicleController.class);
            controller.setVehicleId(uuid);
            vehicles.put(uuid, controller);
        });
    }

    public KafkaVehicleController findVehicleById(String uuid) {
        return this.vehicles.get(uuid);
    }

    public void eachVehicle(Consumer<KafkaVehicleController> consumer) {
        this.vehicles.forEach((uuid, vehicle) -> {
            consumer.accept(vehicle);
        });
    }

}
