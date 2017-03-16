package de.msg.iot.anki.scenario;


import de.msg.iot.anki.controller.kafka.KafkaVehicleController;
import de.msg.iot.anki.entity.Setup;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractScenario implements Scenario {

    protected final Setup setup;
    protected final Map<String, KafkaVehicleController> controller;

    protected AbstractScenario(Setup setup) {
        this.setup = setup;
        this.controller = new HashMap<>();
        this.setup.getVehicles().forEach(vehicle -> {
            this.controller.put(vehicle.getUuid(), new KafkaVehicleController(vehicle.getUuid()));
        });
    }


}
