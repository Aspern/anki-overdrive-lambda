package de.msg.iot.anki.data;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class BatchView {

    @Id
    private String id;
    private int piece;
    private double speed;
    private int lane;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getPiece() {
        return piece;
    }

    public void setPiece(int piece) {
        this.piece = piece;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double optimalSpeed) {
        this.speed = optimalSpeed;
    }

    public int getLane() {
        return lane;
    }

    public void setLane(int lane) {
        this.lane = lane;
    }
}
