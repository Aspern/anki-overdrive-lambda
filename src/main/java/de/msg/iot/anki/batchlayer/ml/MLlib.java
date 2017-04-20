package de.msg.iot.anki.batchlayer.ml;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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

        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("timestamp")
                        .gte("2017-04-18T00:00:00.000Z")
                        .lte("now"));

        SparkConf conf = new SparkConf().setAppName("ml-test")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


        Configuration mongoConf = new Configuration();
        mongoConf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
        mongoConf.set("mongo.input.uri", "mongodb://localhost:27017/anki.AggregateQualityForLocations");
        mongoConf.set("mongo.input.fields", "{position:1,speed:1,quality:1}");

        JavaPairRDD documents = sc.newAPIHadoopRDD(
                mongoConf,
                MongoInputFormat.class,
                Object.class,
                BSONObject.class
        );

        JavaPairRDD data = documents.filter(tuple -> {
            BSONObject document = (BSONObject) ((Tuple2) tuple)._2();
            return (document.get("position")).equals("17:35");
        });


        JavaRDD trainingData = data.map(new Function<Tuple2, LabeledPoint>() {
            @Override
            public LabeledPoint call(Tuple2 tuple) throws Exception {
                BasicDBObject source = (BasicDBObject) tuple._2();

                return new LabeledPoint(
                        (int) source.get("speed"),
                        Vectors.dense(
                                (double) source.get("quality")
                        )
                );

            }
        });

        trainingData.cache();

        int iterations = 100;
        double stepSize = 0.00000001;

        final LinearRegressionModel model = LinearRegressionWithSGD.train(
                JavaRDD.toRDD(trainingData),
                iterations,
                stepSize
        );


        System.out.println(model.predictPoint(
                Vectors.dense(0.8),
                Vectors.dense(1),
                0
        ));


//        ELASTICSEARCH CONFIGURATION
//        Configuration hadoopConfiguration = new Configuration();
//        hadoopConfiguration.set("es.resource", "anki/positionUpdate");
//        hadoopConfiguration.set("es.query", query.toString());
//
//        JavaPairRDD data = sc.newAPIHadoopRDD(
//                hadoopConfiguration,
//                EsInputFormat.class,
//                Text.class,
//                MapWritable.class);

    }

}
