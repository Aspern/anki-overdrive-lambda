package de.msg.iot.anki.controller;

public interface VehicleController {

    void connect();

    void disconnect();

    void setSpeed(int speed, int acceleration);

    default void setSpeed(int speed) {
        setSpeed(speed, 500);
    }

    void setOffset(float offset);

    void changeLane(float offset);

}
