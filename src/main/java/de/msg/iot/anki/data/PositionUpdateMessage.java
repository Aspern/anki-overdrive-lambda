package de.msg.iot.anki.data;

import javax.persistence.Entity;

@Entity
public class PositionUpdateMessage extends VehicleMessage {

    private int location;
    private int piece;
    private float offset;
    private int speed;
    private byte flags;
    private byte lastLangeChangeCmd;
    private byte lastExecLaneChangeCmd;
    private int lastDesiredHorizontalSpeed;
    private int lastDesiredSpeed;

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
}
