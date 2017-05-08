package de.msg.iot.anki.batchlayer.ml;

import com.google.common.util.concurrent.AtomicDouble;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.HashMap;
import java.util.Map;


public class MLlib {


    private static final Map<String, Double> index = new HashMap<>();
    private static final AtomicDouble counter = new AtomicDouble();

    public static void main(String[] args) throws Exception {

        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("timestamp")
                        .gte("2017-04-18T00:00:00.000Z")
                        .lte("now"));


        SparkSession spark = SparkSession.builder()
                .appName("Anki")
                .master("local")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/anki.AggregateQualityForLocations")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());


        JavaMongoRDD<Document> data = MongoSpark.load(jsc);


        data.foreach(document -> {
                    if (!index.containsKey(document.getString("position")))
                        index.put(document.getString("position"), counter.getAndAdd(1.0));
                }
        );


        JavaRDD<LabeledPoint> trainingData = data.map(document ->
                new LabeledPoint(
                        document.getInteger("speed"),
                        Vectors.dense(
                                document.getDouble("quality"),
                                index.get(
                                        document.getString("position")
                                )
                        )
                )
        );


        trainingData.cache();

        LinearRegressionModel model = LinearRegressionWithSGD.train(trainingData.rdd(), 5000, 0.1);
        System.out.println(model.predict(Vectors.dense(0.95, index.get("33:15"))));


    }


}
