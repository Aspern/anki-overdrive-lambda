package de.msg.iot.anki.batchlayer.ml;

import weka.classifiers.Evaluation;
import weka.classifiers.functions.LinearRegression;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MachineLearning implements Runnable {

    @Override
    public void run() {
        try {

            DataSource source = new DataSource("/home/aweber/1-commands/mongo/location-quality.arff");
            final Instances data = source.getDataSet();

            DataSource testSource = new DataSource("/home/aweber/1-commands/mongo/quality-location-validation.arff");
            final Instances test = testSource.getDataSet();

            data.setClassIndex(1);

            LinearRegression model = new LinearRegression();
            model.setOutputAdditionalStats(true);
            model.buildClassifier(data);

            Evaluation evaluation = new Evaluation(data);
            evaluation.evaluateModel(model, data);

            final AtomicInteger i = new AtomicInteger();
            test.forEach(instance -> {
                try {
                    final String out = "position(" + instance.attribute(0).value(i.getAndIncrement()) + ") =" + model.classifyInstance(instance) + "mm/sec";
                    System.out.println(out);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });


        } catch (Exception e) {
            //TODO: Handle errors
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        final ExecutorService threadpool = Executors.newSingleThreadExecutor();
        threadpool.submit(new MachineLearning());
        threadpool.shutdown();
        threadpool.awaitTermination(1, TimeUnit.MINUTES);
    }

}
