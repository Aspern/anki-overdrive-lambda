package de.msg.iot.anki.batchlayer.ml;

import com.mongodb.BasicDBObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.bson.BSONObject;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import scala.Tuple2;


public class MLlib {

    public static void main(String[] args) throws Exception {

//        QueryBuilder query = QueryBuilders.boolQuery()
//                .must(QueryBuilders.rangeQuery("timestamp")
//                        .gte("2017-04-18T00:00:00.000Z")
//                        .lte("now"));
//
//        SparkConf conf = new SparkConf().setAppName("ml-test")
//                .setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//
//        Configuration mongoConf = new Configuration();
//        mongoConf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
//        mongoConf.set("mongo.input.uri", "mongodb://localhost:27017/anki.AggregateQualityForLocations");
//        mongoConf.set("mongo.input.fields", "{position:1,speed:1,quality:1}");
//
//        JavaPairRDD documents = sc.newAPIHadoopRDD(
//                mongoConf,
//                MongoInputFormat.class,
//                Object.class,
//                BSONObject.class
//        );
//
//        JavaPairRDD data = documents.filter(tuple -> {
//            BSONObject document = (BSONObject) ((Tuple2) tuple)._2();
//            return (document.get("position")).equals("17:35");
//        });
//
//
//        JavaRDD<LabeledPoint> trainingData = data.map(new Function<Tuple2, LabeledPoint>() {
//            @Override
//            public LabeledPoint call(Tuple2 tuple) throws Exception {
//                BasicDBObject source = (BasicDBObject) tuple._2();
//
//                return new LabeledPoint(
//                        (double) ((int) source.get("speed")) / 1000d,
//                        Vectors.dense(
//                                (double) source.get("quality")
//                        )
//                );
//
//            }
//        });
//
//        trainingData.cache();
//
//        int iterations = 100;
//        double stepSize = 0.00000001;
//
//
//        final LinearRegressionWithSGD sgd = new LinearRegressionWithSGD(0.00000001d, 100,1, 1);
//        sgd.setIntercept(true);
//
//        final LinearRegressionModel model = sgd.run(trainingData.rdd());
//
//
//        JavaRDD<Tuple2<Object, Object>> valuesAndPred = trainingData
//                .map(point -> new Tuple2<>(point.label(), model
//                        .predict(point.features())));
//
//
//        RegressionMetrics metrics = new RegressionMetrics(valuesAndPred.rdd());
//
//
//        // Squared error
//        System.out.format("MSE = %f\n", metrics.meanSquaredError());
//        System.out.format("RMSE = %f\n", metrics.rootMeanSquaredError());
//
//        // R-squared
//        System.out.format("R Squared = %f\n", metrics.r2());
//
//        // Mean absolute error
//        System.out.format("MAE = %f\n", metrics.meanAbsoluteError());
//
//        // Explained variance
//        System.out.format("Explained Variance = %f\n", metrics.explainedVariance());
//
//        Vector vector = Vectors.dense(0.45);
//        System.out.println(model.predictPoint(vector, model.weights(), model.intercept()));
//
//
////        ELASTICSEARCH CONFIGURATION
////        Configuration hadoopConfiguration = new Configuration();
////        hadoopConfiguration.set("es.resource", "anki/positionUpdate");
////        hadoopConfiguration.set("es.query", query.toString());
////
////        JavaPairRDD data = sc.newAPIHadoopRDD(
////                hadoopConfiguration,
////                EsInputFormat.class,
////                Text.class,
////                MapWritable.class);
//
//    }
    }

}
