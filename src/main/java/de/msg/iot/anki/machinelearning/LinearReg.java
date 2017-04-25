package de.msg.iot.anki.machinelearning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

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
            //System.out.println(parts[1] + " -- " + parts[2]);
            return new LabeledPoint(Integer.parseInt(parts[1]), Vectors.dense(Double.parseDouble(parts[2])));
        });

        parsedData.cache();

        LinearRegressionModel model = LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), 5000, 0.1);

        System.out.println("Predict : " + model.predict(Vectors.dense(0.06)));

    }
}
