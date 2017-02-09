package de.msg.iot.anki.connector;


import java.util.function.Consumer;

public interface Receiver<D> extends Runnable {

    Receiver onReceive(Consumer<D> consumer);

}
