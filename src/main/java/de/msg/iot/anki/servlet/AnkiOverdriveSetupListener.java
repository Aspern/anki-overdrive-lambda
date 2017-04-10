package de.msg.iot.anki.servlet;

import de.msg.iot.anki.entity.Setup;
import de.msg.iot.anki.kafka.AbstractKafkaConsumer;
import org.apache.log4j.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by aweber on 15.03.17.
 */
public class AnkiOverdriveSetupListener implements ServletContextListener {

    private final ExecutorService threadpool = Executors.newSingleThreadExecutor();
    private final Logger logger = Logger.getLogger(AnkiOverdriveSetupListener.class);
    private final EntityManagerFactory factory = Persistence.createEntityManagerFactory("anki");
    private final EntityManager manager = factory.createEntityManager();
    private AbstractKafkaConsumer<Setup> listener;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        logger.info("Starting consumer for AnkiOverdriveSetupListener...");
        this.listener = new AbstractKafkaConsumer<Setup>("setup") {

            @Override
            public Class<Setup> getType() {
                return Setup.class;
            }

            @Override
            public void handle(Setup setup) {
                try {
                    if (setup.isOnline()) {
                        if (manager.createQuery("select d from Setup d where d.ean = '" + setup.getEan() + "'")
                                .getResultList()
                                .isEmpty()) {
                            manager.getTransaction().begin();
                            manager.persist(setup);
                            manager.getTransaction().commit();
                            logger.info("Stored setup with uuid [" + setup.getEan() + "].");
                        } else {
                            logger.warn("Setup with uuid [" + setup.getEan() + "] already exists!");
                        }
                    } else {

                        ((List<Setup>) manager.createQuery("select d from Setup d where d.ean = '" + setup.getEan() + "'")
                                .getResultList()).forEach(record -> {
                            manager.getTransaction().begin();
                            manager.remove(record);
                            manager.getTransaction().commit();
                            logger.info("Deleted setup with uuid [" + record.getEan() + "].");
                        });
                    }

                } catch (Exception e) {
                    logger.error(e);
                }
            }
        };

        threadpool.submit(listener);

    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        logger.info("Shutting down consumer for AnkiOverdriveSetupListener...");
        try {
            listener.stop();
            threadpool.shutdown();
            threadpool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }
}
