package de.msg.iot.anki.connector;

import com.google.inject.Inject;
import com.google.inject.Injector;
import de.msg.iot.anki.batchlayer.masterdata.MasterDataSet;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ConnectorServer {

    private final ExecutorService pool = Executors.newSingleThreadExecutor();

    @Inject
    private Receiver receiver;

    @Inject
    private MasterDataSet masterDataSet;


    public void start() {
        System.out.println("Staring ConnectorServer...");

        receiver.onReceive((message) -> {
            masterDataSet.store(message);
        });

        pool.submit(receiver);
    }

    public void stop() {
        System.out.println("Stopping ConnectorServer...");
        this.pool.shutdownNow();

        try {
            this.masterDataSet.close();
        } catch (Exception e) {
            System.err.println(e);
        }
    }

}
