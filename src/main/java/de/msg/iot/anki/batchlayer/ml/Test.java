package de.msg.iot.anki.batchlayer.ml;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by aweber on 10.04.17.
 */
public class Test {

    private final static ExecutorService pool = Executors.newFixedThreadPool(2);
    private static final MessageQualityPreprocessor p1 = new MessageQualityPreprocessor();
    private static final AntiCollisionPreprocessor p2 = new AntiCollisionPreprocessor();

    public static void main(String[] args) throws InterruptedException {

        pool.submit(p1);
        //pool.submit(p2);
        pool.shutdown();
        Thread.sleep(5000L);
        p1.stop();
        //p2.stop();
        pool.awaitTermination(10L, TimeUnit.MINUTES);

    }

}
