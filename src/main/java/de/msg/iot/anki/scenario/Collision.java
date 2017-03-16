package de.msg.iot.anki.scenario;


import de.msg.iot.anki.entity.Setup;

public class Collision extends AbstractScenario {

    public Collision(Setup setup) {
        super(setup);
    }

    @Override
    public Void call() throws Exception {
        return null;
    }

    @Override
    public String getName() {
        return "collision";
    }
}
