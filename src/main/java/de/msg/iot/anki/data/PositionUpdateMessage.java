package de.msg.iot.anki.data;

import javax.persistence.*;
import java.util.Date;
import java.util.List;

@Entity
public class PositionUpdateMessage {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long uuid;
    private Date timestamp;
    private int id;
    private String vehicleId;
    private int location;
    private int piece;
    private int position;
    private int lane;
    private float offset;
    private int speed;
    private byte flags;
    private byte lastLangeChangeCmd;
    private byte lastExecLaneChangeCmd;
    private int lastDesiredHorizontalSpeed;
    private int lastDesiredSpeed;
    @OneToMany
    private List<Distance> distances;

    public long getUuid() {
        return uuid;
    }

    public void setUuid(long uuid) {
        this.uuid = uuid;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public int getLocation() {
        return location;
    }

    public void setLocation(int location) {
        this.location = location;
    }

    public int getPiece() {
        return piece;
    }

    public void setPiece(int piece) {
        this.piece = piece;
    }

    public float getOffset() {
        return offset;
    }

    public void setOffset(float offset) {
        this.offset = offset;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public byte getFlags() {
        return flags;
    }

    public void setFlags(byte flags) {
        this.flags = flags;
    }

    public byte getLastLangeChangeCmd() {
        return lastLangeChangeCmd;
    }

    public void setLastLangeChangeCmd(byte lastLangeChangeCmd) {
        this.lastLangeChangeCmd = lastLangeChangeCmd;
    }

    public byte getLastExecLaneChangeCmd() {
        return lastExecLaneChangeCmd;
    }

    public void setLastExecLaneChangeCmd(byte lastExecLaneChangeCmd) {
        this.lastExecLaneChangeCmd = lastExecLaneChangeCmd;
    }

    public int getLastDesiredHorizontalSpeed() {
        return lastDesiredHorizontalSpeed;
    }

    public void setLastDesiredHorizontalSpeed(int lastDesiredHorizontalSpeed) {
        this.lastDesiredHorizontalSpeed = lastDesiredHorizontalSpeed;
    }

    public int getLastDesiredSpeed() {
        return lastDesiredSpeed;
    }

    public void setLastDesiredSpeed(int lastDesiredSpeed) {
        this.lastDesiredSpeed = lastDesiredSpeed;
    }

    public List<Distance> getDistances() {
        return distances;
    }

    public void setDistances(List<Distance> distances) {
        this.distances = distances;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getLane() {
        return lane;
    }

    public void setLane(int lane) {
        this.lane = lane;
    }
}
