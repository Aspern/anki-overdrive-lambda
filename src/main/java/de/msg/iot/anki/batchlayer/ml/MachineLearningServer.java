package de.msg.iot.anki.batchlayer.ml;


import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MachineLearningServer {

    private final ExecutorService pool;
    private volatile boolean running = false;
    private final Map<Integer, MachineLearningWorker> workers;
    private final Injector injector;
    private final Logger logger = Logger.getLogger(MachineLearningServer.class);

    @Inject
    public MachineLearningServer(Injector injector) {

        this.pool = Executors.newCachedThreadPool();
        this.workers = new HashMap<>();
        this.injector = injector;
    }

    public void start() {
        if (!this.running) {
            logger.info("Starting MachineLearningServer...");
            this.workers.forEach((piece, worker) -> {
                this.pool.submit(worker);
            });
            this.running = true;
            logger.info("MachineLearningServer running.");
        }
    }

    public void stop() {
        if (this.running) {
            logger.info("Shutting down MachineLearningServer...");
            this.pool.shutdownNow();
            this.running = false;
            logger.info("MachineLearningServer stopped.");
        }
    }


    public void addWorker(int piece) {
        MachineLearningWorker worker = this.injector
                .getInstance(MachineLearningWorker.class)
                .setPiece(piece);

        logger.info("Added MachineLearningWorker worker for piece [" + piece + "].");

        this.workers.put(piece, worker);
    }

}
