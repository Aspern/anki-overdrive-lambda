package de.msg.iot.anki.batchlayer.ml;

import com.google.inject.Inject;
import de.msg.iot.anki.data.BatchView;
import de.msg.iot.anki.settings.Settings;
import org.apache.log4j.Logger;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.LinearRegression;
import weka.core.Instances;
import weka.experiment.InstanceQuery;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

public class MachineLearningWorker implements Runnable {

    private volatile boolean inProgress = true;
    private LinearRegression model;
    private double correlationCoefficient;
    private final Settings settings;
    private int piece;
    private InstanceQuery query;
    private final Logger logger = Logger.getLogger(MachineLearningWorker.class);
    private final EntityManager manager;

    @Inject
    public MachineLearningWorker(Settings settings, EntityManagerFactory factory) throws Exception {
        this.settings = settings;
        this.manager = factory.createEntityManager();
    }


    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Instances instances = this.query.retrieveInstances();
                instances.setClassIndex(1);

                LinearRegression model = new LinearRegression();
                model.setOutputAdditionalStats(true);
                model.buildClassifier(instances);

                Evaluation evaluation = new Evaluation(instances);
                evaluation.evaluateModel(model, instances);
                double correlationCoefficient = evaluation.correlationCoefficient();

                if (this.model == null) {
                    this.model = model;
                    this.correlationCoefficient = correlationCoefficient;
                } else if (this.correlationCoefficient < correlationCoefficient) {
                    this.model = model;
                    this.correlationCoefficient = correlationCoefficient;
                }

                this.inProgress = false;
                updateBatchView();
                Thread.sleep(10000);
            } catch (Exception e) {
                logger.error("Error while creating or evaluating model.", e);
            }
        }
    }

    private void updateBatchView() {
        try {
            for (int lane = 0; lane < 16; ++lane) {
                double[] coeffs = this.model.coefficients();
                String id = "" + piece + lane;

                BatchView view = this.manager.find(BatchView.class, id);

                if (view == null) {
                    view = new BatchView();
                    view.setId("" + piece + lane);
                    this.manager.getTransaction().begin();
                    this.manager.persist(view);
                    this.manager.getTransaction().commit();
                }

                this.manager.getTransaction().begin();
                view.setLane(lane);
                view.setPiece(this.piece);
                view.setSpeed(coeffs[0] * lane + coeffs[2]);
                this.manager.getTransaction().commit();
            }
        } catch (Exception e) {
            logger.error("Error while updating batch view.", e);
        }
    }

    public MachineLearningWorker setPiece(int piece) {
        try {
            this.piece = piece;
            this.query = new InstanceQuery();
            this.query.setUsername(settings.get("mysql.user"));
            this.query.setPassword(settings.get("mysql.password"));
            this.query.setQuery("SELECT lane,speed FROM "
                    + settings.get("mysql.ml.table")
                    + " WHERE piece = "
                    + piece
            );
        } catch (Exception e) {
            logger.error("Error while fetching instances.", e);
        }
        return this;
    }


}
