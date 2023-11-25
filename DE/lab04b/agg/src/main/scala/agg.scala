import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger


object agg {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.sql.session.timeZone", "UTC")
      .appName("lab04b")
      .getOrCreate()

    val readKafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1.newprolab.com:6667",
      "subscribe" -> "aigul_sibgatullina",
      "startingOffsets" -> "earliest"
    )

    val writeKafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1.newprolab.com:6667",
      "topic" -> "aigul_sibgatullina_lab04b_out"
    )

    val jsonSchema = new StructType()
      .add("event_type", StringType)
      .add("category", StringType)
      .add("item_id", StringType)
      .add("item_price", IntegerType)
      .add("uid", StringType)
      .add("timestamp", LongType)

    val sdf = spark.readStream.format("kafka").options(readKafkaParams).load

    def processBatch(df: DataFrame, batchId: Long): Unit = {
      val finalRes = df
        .select(from_json(col("value").cast("string"), jsonSchema).alias("data"))
        .withColumn("timestamp_s", (col("data.timestamp") / 1000).cast("long"))
        .groupBy(window(from_unixtime(col("timestamp_s")), "1 hour").alias("time_window"))
        .agg(
          sum(when(col("data.event_type") === "buy", col("data.item_price"))).alias("revenue"),
          count(when(col("data.event_type") === "buy", col("data.item_price"))).alias("purchases"),
          count(when(col("data.uid").isNotNull, col("data.item_price"))).alias("visitors")
        )
        .select(
          unix_timestamp(col("time_window.start")).alias("start_ts"),
          unix_timestamp(col("time_window.end")).alias("end_ts"),
          col("revenue"),
          col("visitors"),
          col("purchases"),
          (col("revenue") / col("purchases")).alias("aov")
        )

      val finalResJson = finalRes.select(to_json(struct(finalRes.columns.map(col): _*)).alias("value"))

      finalResJson
        .write
        .format("kafka")
        .options(writeKafkaParams)
        .mode("append")
        .save()
    }

    sdf
      .writeStream
      .foreachBatch(processBatch _)
      .trigger(Trigger.ProcessingTime("1 minute"))
      .option("checkpointLocation", "streaming/chk/chk_kafka_de")
      .outputMode("update")
      .start()
      .awaitTermination()
  }
}
