package de.msg.iot.anki.machinelearning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import scala.Tuple2;

/**
 * Created by msg on 25.04.17.
 */
public class LinearReg {

    public static void main(String[] args){

        String path = "/home/msg/Documents/location-quality.txt";

        
        // Define the configuration for spark context
        SparkConf sparkConf = new SparkConf().setAppName("AnkiLambda").setMaster("local[*]");

        // Initialize the spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> data = sc.textFile(path);

        JavaRDD<LabeledPoint> parsedData = data.map(a -> {

            String[] parts = a.split("\t");
            return new LabeledPoint(Integer.parseInt(parts[1]), Vectors.dense(
                    Double.parseDouble(parts[2])) // Quality
            );
        });

        parsedData.cache();

        LinearRegressionModel model = LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), 5000, 0.1);

        JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData.map(a ->{
            double pred = model.predict(a.features());
            return new Tuple2<>(pred, a.label());
        });

        double MSE = new JavaDoubleRDD(valuesAndPreds.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        return Math.pow(pair._1() - pair._2(), 2.0);
                    }
                }
        ).rdd()).mean();
        System.out.println("training Mean Squared Error = " + MSE);

        System.out.println("Predict : " + model.predict(Vectors.dense(0.6)));

    }
}
