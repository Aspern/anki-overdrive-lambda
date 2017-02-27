package de.msg.iot.anki.controller.kafka;


public class Command {
    private final String name;
    private final Number[] params;

    public Command(String name, Number[] params) {
        this.name = name;
        this.params = params;
    }

    public String getName() {
        return name;
    }

    public Number[] getParams() {
        return params;
    }
}
