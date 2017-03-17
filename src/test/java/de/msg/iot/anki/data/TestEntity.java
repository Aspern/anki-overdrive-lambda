package de.msg.iot.anki.data;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

@Entity
public class TestEntity implements Data {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private String id;
    private Date timestamp;
    private String value;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public static TestEntity create() {
        TestEntity entity = new TestEntity();
        entity.setTimestamp(new Date());
        String randomValue = String.valueOf(ThreadLocalRandom
                .current()
                .nextDouble());
        entity.setValue(randomValue);
        return entity;
    }
}
