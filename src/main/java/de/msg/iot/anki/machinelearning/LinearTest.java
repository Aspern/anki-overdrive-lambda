package de.msg.iot.anki.machinelearning;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
/**
 * Created by msg on 25.04.17.
 */
public class LinearTest {

    public static void main(String[] args){

        // Define the configuration for spark context
        SparkConf sparkConf = new SparkConf().setAppName("AnkiLambda").setMaster("local[*]");

        // Initialize the spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String path = "/home/msg/Documents/lpsa.data";
        JavaRDD<String> data = sc.textFile(path);
        JavaRDD<LabeledPoint> parsedData = data.map(
                new Function<String, LabeledPoint>() {
                    public LabeledPoint call(String line) {
                        String[] parts = line.split(",");
                        String[] features = parts[1].split(" ");
                        double[] v = new double[features.length];
                        for (int i = 0; i < features.length - 1; i++) {
                            v[i] = Double.parseDouble(features[i]);
                        }
                        return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
                    }
                }
        );
        parsedData.cache();

// Building the model
        int numIterations = 100;
        double stepSize = 0.00000001;
        final LinearRegressionModel model =
                LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations, stepSize);

// Evaluate model on training examples and compute training error
        JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData.map(
                new Function<LabeledPoint, Tuple2<Double, Double>>() {
                    public Tuple2<Double, Double> call(LabeledPoint point) {
                        double prediction = model.predict(point.features());
                        return new Tuple2<>(prediction, point.label());
                    }
                }
        );
        double MSE = new JavaDoubleRDD(valuesAndPreds.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        return Math.pow(pair._1() - pair._2(), 2.0);
                    }
                }
        ).rdd()).mean();
        System.out.println("training Mean Squared Error = " + MSE);
    }

}
