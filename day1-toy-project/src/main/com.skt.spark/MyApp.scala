import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.format_number

object MyApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkstudy").master("local").getOrCreate()

    val jsonDf = spark.read.option("multiLine", true).json("/Users/ohsch/Downloads/AptTrade.json").cache()
//    jsonDf.printSchema()
//    jsonDf.registerTempTable("AptTrade")

    val items = sqlContext.sql("""
      select items.items.* from (select explode(body.items) as items from (select AptTrade.body from AptTrade) body) items
    """)

//    items.show
//    items.printSchema()

    val castingItems = items.withColumn("거래금액", regexp_replace(col("거래금액"), ",", "").cast(IntegerType))
    val orderItems = castingItems.orderBy(col("거래금액").desc)
    val finalItem = orderItems.take(10).last
    val money = finalItem.getAs[Int]("거래금액");
    val dong = finalItem.getAs[String]("법정동");
    val aptNm = finalItem.getAs[String]("아파트");
    println(s"거래금액 : ${money}, 아파트 : ${dong} ${aptNm}")
  }
}