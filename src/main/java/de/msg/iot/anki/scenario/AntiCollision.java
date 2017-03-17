package de.msg.iot.anki.scenario;


import com.google.inject.Inject;
import com.google.inject.Injector;
import de.msg.iot.anki.controller.VehicleController;
import de.msg.iot.anki.settings.Settings;

public class AntiCollision implements Runnable, Scenario {

    private VehicleController skull;
    private VehicleController groundShock;

    @Inject
    public AntiCollision(Injector injector, Settings settings) {
        this.skull = injector.getInstance(VehicleController.class);
        this.groundShock = injector.getInstance(VehicleController.class);

        this.skull.setVehicleId(settings.get("vehicle.skull.id"));
        this.groundShock.setVehicleId(settings.get("vehicle.groundshock.id"));
    }


    @Override
    public void run() {

        try {

            this.skull.connect();
            this.groundShock.connect();

            Thread.sleep(2000);

            this.skull.setOffset(-68.0f);
            this.groundShock.setOffset(68.0f);
            this.skull.setSpeed(400, 200);
            this.groundShock.setSpeed(600, 200);

        } catch (Exception e) {
            e.printStackTrace();
        }

        while (!Thread.currentThread().interrupted()) {

        }
    }

    @Override
    public void start() {
        
    }

    @Override
    public void stop() {

    }
}
