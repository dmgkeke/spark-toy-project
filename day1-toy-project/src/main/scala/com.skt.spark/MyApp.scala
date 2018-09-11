package com.skt.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType


object MyApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkstudy").master("local").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    sc.setLogLevel("ERROR")

    val path = if( args.size > 0 ) {
      args(0)
    } else {
      "/Users/schoh/Downloads/AptTrade.json"
    }
    val jsonDf = spark.read.option("multiLine", true).json(path).cache()
    jsonDf.registerTempTable("AptTrade")

    val items = sqlContext.sql("""
      select items.items.* from (select explode(body.items) as items from (select AptTrade.body from AptTrade) body) items
    """)

    val castingItems = items.withColumn("거래금액", regexp_replace(col("거래금액"), ",", "").cast(IntegerType))
    val orderItems = castingItems.orderBy(col("거래금액").desc)
    val finalItem = orderItems.take(10).last
    val money = finalItem.getAs[Int]("거래금액");
    val dong = finalItem.getAs[String]("법정동");
    val aptNm = finalItem.getAs[String]("아파트");
    val area = finalItem.getAs[String]("전용면적");
    println(s"${dong} ${aptNm} ( 전용면적(m^2) : ${area}, 거래금액(만원) : ${money} ) )")
  }
}