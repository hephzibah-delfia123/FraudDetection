package com.hephzibah.spark.jobs

import com.hephzibah.cassandra.CassandraConfig
import com.hephzibah.config.Config
import com.hephzibah.spark.{DataBalancing, DataReader, SparkConfig}
import com.hephzibah.spark.algorithms.Algorithms
import com.hephzibah.spark.pipeline.BuildPipeline
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType




/**
  * Created by kafka on 9/5/18.
  */

object FraudDetectionTraining extends SparkJob("Balancing Fraud & Non-Fraud Dataset"){

  val logger = Logger.getLogger(getClass.getName)



  def main(args: Array[String]) {

    Config.parseArgs(args)

    import sparkSession.implicits._

    val fraudTransactionDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.fraudTransactionTable)
      .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud")

    val nonFraudTransactionDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.nonFraudTransactionTable)
      .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud")

    val transactionDF = nonFraudTransactionDF.union(fraudTransactionDF)
    transactionDF.cache()


    transactionDF.show(false)


    val coloumnNames = List("category", "merchant", "distance", "amt", "age")


    val pipelineStages = BuildPipeline.createFeaturePipeline(transactionDF.schema, coloumnNames)
    val pipeline = new Pipeline().setStages(pipelineStages)
    val preprocessingTransformerModel = pipeline.fit(transactionDF)


    val featureDF = preprocessingTransformerModel.transform(transactionDF)


    featureDF.show(false)


    val Array(trainData, testData) = featureDF.randomSplit(Array(0.8, 0.2))

    val featureLabelDF = trainData.select("features", "is_fraud").cache()

    val nonFraudDF = featureLabelDF.filter($"is_fraud" === 0)


    val fraudDF = featureLabelDF.filter($"is_fraud" === 1)
    val fraudCount = fraudDF.count()


    println("fraudCount: " + fraudCount)


    /* There will be very few fraud transaction and more normal transaction. Models created  from such
     * imbalanced data will not have good prediction accuracy. Hence balancing the dataset. K-means is used for balancing
     */
    val balancedNonFraudDF = DataBalancing.createBalancedDataframe(nonFraudDF, fraudCount.toInt)

    val finalfeatureDF = fraudDF.union(balancedNonFraudDF)



    val randomForestModel = Algorithms.randomForestClassifier(finalfeatureDF)
    val predictionDF = randomForestModel.transform(testData)
    predictionDF.show(false)


    val predictionAndLabels =
      predictionDF.select(col("prediction"), col("is_fraud").cast(DoubleType)).rdd.map {
        case Row(prediction: Double, label: Double) => (prediction, label)
      }.cache()


    val tp = predictionAndLabels.filter { case (predicted, actual) => actual == 1 && predicted == 1 }.count().toFloat
    val fp = predictionAndLabels.filter { case (predicted, actual) => actual == 0 && predicted == 1 }.count().toFloat
    val tn = predictionAndLabels.filter { case (predicted, actual) => actual == 0 && predicted == 0 }.count().toFloat
    val fn = predictionAndLabels.filter { case (predicted, actual) => actual == 1 && predicted == 0 }.count().toFloat



    printf(s"""|=================== Confusion matrix ==========================
               |#############| %-15s                     %-15s
               |-------------+-------------------------------------------------
               |Predicted = 1| %-15f                     %-15f
               |Predicted = 0| %-15f                     %-15f
               |===============================================================
         """.stripMargin, "Actual = 1", "Actual = 0", tp, fp, fn, tn)


    println()



    val metrics =new MulticlassMetrics(predictionAndLabels)

    /*True Positive Rate: Out of all fraud transactions, how  much we predicted correctly. It should be high as possible.*/
    println("True Positive Rate: " + tp/(tp + fn))  // tp/(tp + fn)

    /*Out of all the genuine transactions(not fraud), how much we predicted wrong(predicted as fraud). It should be low as possible*/
    println("False Positive Rate: " + fp/(fp + tn))

    println("Precision: " +  tp/(tp + fp))

    /* Save Preprocessing  and Random Forest Model */
    randomForestModel.save(SparkConfig.modelPath)
    preprocessingTransformerModel.save(SparkConfig.preprocessingModelPath)

  }

}
