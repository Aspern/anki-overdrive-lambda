package de.msg.iot.anki.data;


import javax.persistence.*;

@Entity
public class Distance implements Data {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private String id;
    private String vehicle;
    private float horizontal;
    private float vertical;
    private float delta;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getVehicle() {
        return vehicle;
    }

    public void setVehicle(String vehicle) {
        this.vehicle = vehicle;
    }

    public float getHorizontal() {
        return horizontal;
    }

    public void setHorizontal(float horizontal) {
        this.horizontal = horizontal;
    }

    public float getVertical() {
        return vertical;
    }

    public void setVertical(float vertical) {
        this.vertical = vertical;
    }

    public float getDelta() {
        return delta;
    }

    public void setDelta(float delta) {
        this.delta = delta;
    }
}
