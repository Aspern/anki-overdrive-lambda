package de.msg.iot.anki.entity;

import javax.persistence.*;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
@Entity
public class Setup {

    @Id
    private String uuid;
    @OneToMany(cascade = CascadeType.PERSIST, orphanRemoval = true)
    private List<Vehicle> vehicles;
    @OneToOne(cascade = CascadeType.PERSIST, orphanRemoval = true)
    private Track track;
    private boolean online;

    public List<Vehicle> getVehicles() {
        return vehicles;
    }

    public void setVehicles(List<Vehicle> vehicles) {
        this.vehicles = vehicles;
    }

    public Track getTrack() {
        return track;
    }

    public void setTrack(Track track) {
        this.track = track;
    }

    public boolean isOnline() {
        return online;
    }

    public void setOnline(boolean online) {
        this.online = online;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
}
