package de.msg.iot.anki.scenario;

import de.msg.iot.anki.entity.Setup;

public class AntiCollision extends AbstractScenario {

    public AntiCollision(Setup setup) {
        super(setup);
    }

    @Override
    public Void call() throws Exception {
        return null;
    }

    @Override
    public String getName() {
        return "anti-collision";
    }
}
