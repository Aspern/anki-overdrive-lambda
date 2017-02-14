package de.msg.iot.anki.connector;

import com.google.inject.Inject;
import de.msg.iot.anki.batchlayer.masterdata.MasterDataSet;
import org.apache.log4j.Logger;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ConnectorServer {

    private final ExecutorService pool = Executors.newSingleThreadExecutor();
    private final Logger logger = Logger.getLogger(ConnectorServer.class);
    private volatile boolean running = false;

    @Inject
    private Receiver receiver;

    @Inject
    private MasterDataSet masterDataSet;


    public void start() {
        if (!running) {
            logger.info("Staring ConnectorServer...");
            receiver.onReceive((message) -> {
                masterDataSet.store(message);
            });

            pool.submit(receiver);
            running = true;
            logger.info("ConnectorServer running.");
        }
    }

    public void stop() {
        if (running) {
            logger.info("Shutting down connector ConnectorServer...");
            this.pool.shutdownNow();

            try {
                this.masterDataSet.close();
            } catch (Exception e) {
                logger.error("Could not close MasterDataSet properly.", e);
            } finally {
                logger.info("ConnectorServer stopped.");
            }
        }
    }

}
