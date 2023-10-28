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
      "startingOffsets" -> "latest"
    )

    val writeKafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1.newprolab.com:6667",
      "subscribe" -> "aigul_sibgatullina_lab04b_out"
    )

    val jsonSchema = new StructType()
      .add("event_type", StringType)
      .add("category", StringType)
      .add("item_id", StringType)
      .add("item_price", IntegerType)
      .add("uid", StringType)
      .add("timestamp", LongType)

    val sdf = spark.readStream.format("kafka").options(readKafkaParams).load

    val finalRes = sdf
      .select(from_json(col("value").cast("string"), jsonSchema).alias("data"))
      .where(col("data.uid").isNotNull)
      .agg(
        sum(when(col("data.event_type") === "buy", col("data.item_price"))).alias("revenue"),
        count(when(col("data.event_type") === "buy", col("data.item_price"))).alias("purchases"),
        count(when(col("data.event_type") === "view", col("data.item_price"))).alias("visitors"),
        min("data.timestamp").alias("start_ts")
      )
      .select(
        col("start_ts"),
        unix_timestamp(from_unixtime(col("start_ts") / 1000) + expr("INTERVAL 1 HOUR")).alias("end_ts"),
        col("revenue"),
        col("visitors"),
        col("purchases"),
        (col("revenue") / col("purchases")).alias("aov")
      )

    val finalResJson = finalRes.select(to_json(struct(finalRes.columns.map(col): _*)).alias("value"))

    finalResJson
      .writeStream
      .format("kafka")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .options(writeKafkaParams)
      .option("checkpointLocation", "streaming/chk/chk_kafka_de")
      .outputMode("update")
      .start()
  }
}
