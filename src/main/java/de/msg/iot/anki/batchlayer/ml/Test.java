package de.msg.iot.anki.batchlayer.ml;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by aweber on 10.04.17.
 */
public class Test {

    private final static ExecutorService pool = Executors.newSingleThreadExecutor();

    public static void main(String[] args) throws InterruptedException {

        pool.submit(new Preprocessor());
        pool.shutdown();
        pool.awaitTermination(10L, TimeUnit.MINUTES);

    }

}
