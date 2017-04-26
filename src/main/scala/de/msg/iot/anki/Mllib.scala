package de.msg.iot.anki

import com.mongodb.spark.MongoSpark
import org.apache.spark.ml.feature.{LabeledPoint, StringIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

object Mllib {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Anki")
      .master("local")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/anki.AggregateQualityForLocations")
      .getOrCreate()

    val data = MongoSpark.load(spark)

//    import spark.implicits._;
//
//    val filtered = data.drop("_id")
//
//    val indexer = new StringIndexer()
//      .setInputCol("position")
//      .setOutputCol("positionIndex")
//      .fit(filtered)
//
//    val transformed = indexer.transform(filtered)
//      .drop("position")
//
//    transformed.forEach(x => {
//      println(x.equals())
//    })

//    val labeled = transformed.map { x => {
//        LabeledPoint(x.getInt(0).toDouble,
//          Vectors.dense(
//            x.getDouble(1),
//            x.getDouble(2),
//            x.getDouble(3)
//          )
//        )
//      }
//    }
//
//
//    labeled.foreach { x => println(x) }

    spark.stop()


  }
}