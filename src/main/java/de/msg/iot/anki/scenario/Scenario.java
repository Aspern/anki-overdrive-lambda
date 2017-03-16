package de.msg.iot.anki.scenario;

import java.util.concurrent.Callable;

public interface Scenario extends Callable<Void> {

    String getName();

}
