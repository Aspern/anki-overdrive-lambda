package de.msg.iot.anki.machinelearning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by msg on 03.05.17.
 */
public class DecisionT {

    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("JavaDecisionTreeRegressionExample").setMaster("local[*]");;
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String path = "/home/msg/Documents/location-quality.txt";

        JavaRDD<String> data = sc.textFile(path);

        JavaRDD<LabeledPoint> parsedData = data.map(a -> {

            String[] parts = a.split("\t");
            //System.out.println(Double.parseDouble(parts[0].split(":")[0]) + " -- " + parts[1] + " -- " + parts[2]);
            return new LabeledPoint(Integer.parseInt(parts[1]), Vectors.dense(
                    Double.parseDouble(parts[2])) // Quality
            );
        });

        parsedData.cache();

        // Split the data into training and test sets (30% held out for testing)
        JavaRDD<LabeledPoint>[] splits = parsedData.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

// Set parameters.
// Empty categoricalFeaturesInfo indicates all features are continuous.
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        String impurity = "variance";
        Integer maxDepth = 5;
        Integer maxBins = 32;

// Train a DecisionTree model.
        final DecisionTreeModel model = DecisionTree.trainRegressor(parsedData,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);

        System.out.println(model.predict(Vectors.dense(0.1)));




// Evaluate model on test instances and compute test error

        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<>(model.predict(p.features()), p.label());
                    }
                });
        Double testMSE =
                predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {
                    @Override
                    public Double call(Tuple2<Double, Double> pl) {
                        Double diff = pl._1() - pl._2();
                        return diff * diff;
                    }
                }).reduce(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double a, Double b) {
                        return a + b;
                    }
                }) / data.count();


        System.out.println("Test Mean Squared Error: " + testMSE);
        System.out.println("Learned regression tree model:\n" + model.toDebugString());
    }
}
