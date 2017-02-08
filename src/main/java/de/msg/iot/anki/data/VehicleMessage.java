package de.msg.iot.anki.data;

import javax.persistence.*;
import java.util.Date;

@MappedSuperclass
public class VehicleMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long uuid;
    private Date timestamp;
    private int id;
    private String vehicleId;

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
}
