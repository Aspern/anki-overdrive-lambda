package de.msg.iot.anki

import com.mongodb.spark.MongoSpark
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

object Mllib {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Anki")
      .master("local")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/anki.AggregateQualityForLocations")
      .getOrCreate()

    val data = MongoSpark.load(spark)

    val filtered = data.drop("_id")
//
//    val cvModel = new CountVectorizer()
//      .setInputCol("position")
//      .setOutputCol("features")
//      .setVocabSize(3)
//      .setMinDF(2)
//      .fit(filtered)
//
//
//    val indexed = cvModel.transform(filtered)
//
//    indexed.foreach {x => println(x)}

    val df = spark.createDataFrame(Seq(
      (0, Array("a")),
      (1, Array("a"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).show(false)

    spark.stop()


    //    val conf = new SparkConf().setAppName("DecisionTreeRegressionExample").setMaster("local")
    //    val sc = new SparkContext(conf)
    //
    //    val data = MLUtils.loadLibSVMFile(sc, "src/main/resources/sample_libsvm_data.txt")
    //
    //    val splits = data.randomSplit(Array(0.7, 0.3))
    //    val(trainingData, testData) = (splits(0), splits(1))
    //
    //
    //    val categoricalFeaturesInfo = Map[Int, Int]()
    //    val impurity = "variance"
    //    val maxDepth = 5
    //    val maxBins = 32
    //
    //    val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    //
    //    val labelsAndPredictors = testData.map { point =>
    //        val prediction = model.predict(point.features)
    //      (point.label, prediction)
    //    }
    //
    //    model.
    //
    //    val testMSE = labelsAndPredictors.map {case(v,p) => math.pow(v - p, 2)}.mean()
    //
    //    println("Test Mean Squared Error = " + testMSE)
    //    println("Learned regression tree model:\n" + model.toDebugString)
    //
    //
    //
    //    sc.stop()

  }
}