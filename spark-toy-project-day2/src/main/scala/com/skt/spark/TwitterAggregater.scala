package com.skt.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object  TwitterAggregater extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("TwitterStreamPrinter").master("local[*]").getOrCreate()

    val df = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.40.99.52:9092,10.40.99.53:9092,10.40.99.51:9092")
      .option("subscribe", "twitter")
      .option("auto.offset.reset", "earliest")
      .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val mySchema = StructType(Array(
      StructField("channel", StringType),
      StructField("actualChannel", StringType),
      StructField("subscribedChannel", StringType),
      StructField("timetoken", StringType),
      StructField("publisher", StringType),
      StructField("message", StructType(Array(
        StructField("created_at", StringType),
        StructField("id", StringType),
        StructField("id_str", StringType),
        StructField("text", StringType),
        StructField("display_text_range", ArrayType(IntegerType)),
        StructField("source", StringType),
        StructField("truncated", BooleanType),
        StructField("in_reply_to_status_id", StringType),
        StructField("in_reply_to_status_id_str", StringType),
        StructField("in_reply_to_user_id", StringType),
        StructField("in_reply_to_user_id_str", StringType),
        StructField("in_reply_to_screen_name", StringType),
        StructField("user", StructType(Array(
          StructField("id", StringType),
          StructField("id_str", StringType),
          StructField("name", StringType),
          StructField("screen_name", StringType),
          StructField("location", StringType),
          StructField("url", StringType),
          StructField("description", StringType),
          StructField("translator_type", StringType),
          StructField("protected", BooleanType),
          StructField("verified", BooleanType),
          StructField("followers_count", IntegerType),
          StructField("friends_count", IntegerType),
          StructField("listed_count", IntegerType),
          StructField("favourites_count", IntegerType),
          StructField("statuses_count", IntegerType),
          StructField("utc_offset", StringType),
          StructField("time_zone", StringType),
          StructField("geo_enabled", BooleanType),
          StructField("lang", StringType),
          StructField("contributors_enabled", BooleanType),
          StructField("is_translator", BooleanType),
          StructField("profile_background_color", StringType),
          StructField("profile_background_image_url", StringType),
          StructField("profile_background_image_url_https", StringType),
          StructField("profile_background_tile", BooleanType),
          StructField("profile_link_color", StringType),
          StructField("profile_sidebar_border_color", StringType),
          StructField("profile_sidebar_fill_color", StringType),
          StructField("profile_text_color", StringType),
          StructField("profile_use_background_image", BooleanType),
          StructField("profile_image_url", StringType),
          StructField("profile_image_url_https", StringType),
          StructField("profile_banner_url", StringType),
          StructField("default_profile", BooleanType),
          StructField("default_profile_image", BooleanType),
          StructField("following", StringType),
          StructField("follow_request_sent", StringType),
          StructField("notifications", StringType)
        ))),
        StructField("geo", StringType),
        StructField("coordinates", StringType),
        StructField("place", StructType(Array(
          StructField("id", StringType),
          StructField("url", StringType),
          StructField("place_type", StringType),
          StructField("name", StringType),
          StructField("full_name", StringType),
          StructField("country_code", StringType),
          StructField("country", StringType),
          StructField("bounding_box", StructType(Array(
            StructField("type", StringType),
            StructField("coordinates", ArrayType(ArrayType(IntegerType)))
          ))),
          StructField("attributes", StringType)
        ))),
        StructField("contributors", StringType),
        StructField("is_quote_status", BooleanType),
        StructField("quote_count", IntegerType),
        StructField("reply_count", IntegerType),
        StructField("retweet_count", IntegerType),
        StructField("favorite_count", IntegerType),
        StructField("entities", StructType(Array(
          StructField("hashtags", ArrayType(StringType)),
          StructField("urls", ArrayType(StringType)),
          StructField("user_mentions", ArrayType(StringType)),
          StructField("symbols", ArrayType(StringType)),
          StructField("media", ArrayType(StructType(Array(
            StructField("id", IntegerType),
            StructField("id_str", StringType),
            StructField("indices", ArrayType(IntegerType)),
            StructField("media_url", StringType),
            StructField("media_url_https", StringType),
            StructField("url", StringType),
            StructField("display_url", StringType),
            StructField("expanded_url", StringType),
            StructField("type", StringType),
            StructField("sizes", StructType(Array(
              StructField("thumb", StructType(Array(
                StructField("w", IntegerType),
                StructField("h", IntegerType),
                StructField("resize", StringType)
              ))),
              StructField("large", StructType(Array(
                StructField("w", IntegerType),
                StructField("h", IntegerType),
                StructField("resize", StringType)
              ))),
              StructField("medium", StructType(Array(
                StructField("w", IntegerType),
                StructField("h", IntegerType),
                StructField("resize", StringType)
              ))),
              StructField("small", StructType(Array(
                StructField("w", IntegerType),
                StructField("h", IntegerType),
                StructField("resize", StringType)
              )))
            )))
          ))))
        ))),
        StructField("extended_entities", StructType(Array(
          StructField("media", ArrayType(StructType(Array(
            StructField("id", IntegerType),
            StructField("id_str", StringType),
            StructField("indices", ArrayType(IntegerType)),
            StructField("media_url", StringType),
            StructField("media_url_https", StringType),
            StructField("url", StringType),
            StructField("display_url", StringType),
            StructField("expanded_url", StringType),
            StructField("type", StringType),
            StructField("sizes", StructType(Array(
              StructField("thumb", StructType(Array(
                StructField("w", IntegerType),
                StructField("h", IntegerType),
                StructField("resize", StringType)
              ))),
              StructField("large", StructType(Array(
                StructField("w", IntegerType),
                StructField("h", IntegerType),
                StructField("resize", StringType)
              ))),
              StructField("medium", StructType(Array(
                StructField("w", IntegerType),
                StructField("h", IntegerType),
                StructField("resize", StringType)
              ))),
              StructField("small", StructType(Array(
                StructField("w", IntegerType),
                StructField("h", IntegerType),
                StructField("resize", StringType)
              )))
            )))
          ))))
        ))),
        StructField("favorited", BooleanType),
        StructField("retweeted", BooleanType),
        StructField("possibly_sensitive", BooleanType),
        StructField("filter_level", StringType),
        StructField("lang", StringType),
        StructField("timestamp_ms", StringType)
      )
      ))
    ))

    val query = df.selectExpr("CAST(value AS STRING)").withColumn("value_parse", from_json(col("value"), mySchema))
      .writeStream
      .format("console")
      .start()

    df.printSchema()
    query.awaitTermination()


//    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(60))
//
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "10.40.99.52:9092,10.40.99.53:9092,10.40.99.51:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "com.skt.spark",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    )
//
//    val topics = Array("twitter")
//    val stream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams)
//    )
//
//    stream.map(_.value().toString).print
//
//    ssc.start
//
//    ssc.awaitTermination
  }
}
