package com.hephzibah.spark.jobs

import com.hephzibah.cassandra.{CassandraConfig, CassandraDriver}
import com.hephzibah.config.Config
import com.hephzibah.creditcard.Schema
import com.hephzibah.spark.{DataReader, SparkConfig}
import com.hephzibah.utils.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{TimestampType, IntegerType}

/**
 * Created by kafka on 24/5/18.
 */
object IntialImportToCassandra extends SparkJob("Initial Import to Cassandra"){

  def main(args: Array[String]) {

    Config.parseArgs(args)

    import sparkSession.implicits._

    val customerDF = DataReader.read(SparkConfig.customerDatasource, Schema.customerSchema)
    val customerAgeDF = customerDF.withColumn("age", (datediff(current_date(),to_date($"dob"))/365).cast(IntegerType))


    val transactionDF = DataReader.read(SparkConfig.transactionDatasouce, Schema.fruadCheckedTransactionSchema)
      .withColumn("trans_date", split($"trans_date", "T").getItem(0))
      .withColumn("trans_time", concat_ws(" ", $"trans_date", $"trans_time"))
      .withColumn("trans_time", to_timestamp($"trans_time", "YYYY-MM-dd HH:mm:ss") cast(TimestampType))


    val distanceUdf = udf(Utils.getDistance _)

    val processedDF = transactionDF.join(broadcast(customerAgeDF), Seq("cc_num"))
      .withColumn("distance", lit(round(distanceUdf($"lat", $"long", $"merch_lat", $"merch_long"), 2)))
      .select("cc_num", "trans_num", "trans_time", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "is_fraud")

    processedDF.cache()

    val fraudDF = processedDF.filter($"is_fraud" === 1)
    val nonFraudDF = processedDF.filter($"is_fraud" === 0)


    /* Save Customer data to cassandra */
    customerDF.write
      .format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(Map("keyspace" -> CassandraConfig.keyspace, "table" -> CassandraConfig.customer))
      .save()

    /* Save fraud transaction data to fraud_transaction cassandra table*/
    fraudDF.write
      .format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(Map("keyspace" -> CassandraConfig.keyspace, "table" -> CassandraConfig.fraudTransactionTable))
      .save()

    /* Save non fraud transaction data to non_fraud_transaction cassandra table*/
    nonFraudDF.write
      .format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(Map("keyspace" -> CassandraConfig.keyspace, "table" -> CassandraConfig.nonFraudTransactionTable))
      .save()


  }

}
