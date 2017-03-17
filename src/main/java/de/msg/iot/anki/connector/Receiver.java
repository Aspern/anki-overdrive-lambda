package de.msg.iot.anki.connector;


import de.msg.iot.anki.data.Data;

import java.util.function.Consumer;

public interface Receiver<D extends Data> extends Runnable {

    Receiver onReceive(Consumer<D> consumer);

}
