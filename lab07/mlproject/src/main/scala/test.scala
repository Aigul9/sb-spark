import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("lab07_test")
      .getOrCreate()

    val modelDir: String = spark.sparkContext.getConf.get(s"spark.model.input_dir")
    val kafkaInput: String = spark.sparkContext.getConf.get(s"spark.kafka.input_topic")
    val kafkaOutput: String = spark.sparkContext.getConf.get(s"spark.kafka.input_topic")

    val readKafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1.newprolab.com:6667",
      "subscribe" -> kafkaInput,
      "startingOffsets" -> "latest"
    )

    val sdf: DataFrame = spark
      .readStream
      .format("kafka")
      .options(readKafkaParams)
      .load()
      .select(col("value").cast("string").alias("value"))

    val weblogsSchema = StructType(
      List(
        StructField("uid", StringType),
        StructField("visits", StringType)
      )
    )

    val visitSchema = ArrayType(MapType(StringType, StringType))

    val testStream: DataFrame = sdf
      .withColumn("value", from_json(col("value"), weblogsSchema))
      .withColumn("visits", col("value.visits"))
      .repartition(200)
      .withColumn("visit", explode(from_json(col("visits"), visitSchema)))
      .withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .groupBy(col("value.uid").alias("uid"))
      .agg(collect_list("domain").alias("domains"))

    val modelSaved = PipelineModel.load(modelDir)

    val predictions = modelSaved
      .transform(testStream)
      .select("to_json(struct(uid, gender_age_prediction as gender_age)) as value")

    val writeKafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1.newprolab.com:6667",
      "topic" -> kafkaOutput
    )

    predictions
      .writeStream
      .format("kafka")
      .options(writeKafkaParams)
      .option("checkpointLocation", "streaming/chk/chk_kafka_de_07")
      .outputMode("update")
      .start()
      .awaitTermination()

  }
}
