package com.skt.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object  TwitterAggregater extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("TwitterStreamPrinter").master("local[*]").getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")

    val mySchema = StructType(List(
      StructField("message", StructType(List(
        StructField("text", StringType)
      )))
    ))

    import sparkSession.implicits._

    val df = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.40.99.52:9092,10.40.99.53:9092,10.40.99.51:9092")
      .option("subscribe", "twitter")
      .option("auto.offset.reset", "latest")
      .load()

    val parseData = df.selectExpr("CAST(value AS STRING) as json")
        .select(from_json($"json", mySchema).as("data"))
        .select("data.message.text")
        .withColumn("text", explode(split($"text", " ")))
        .filter(!isnull($"text"))
        .groupBy($"text")
        .count()
        .sort(desc("count"))


    parseData.printSchema()

    val query = parseData
      .writeStream
      .queryName("messages")
      .format("console")
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }
}