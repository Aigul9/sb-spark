import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.{Pipeline, PipelineModel}

object train {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("lab07_train")
      .getOrCreate()

    val trainDir: String = spark.sparkContext.getConf.get(s"spark.train.input_dir")
    val modelDir: String = spark.sparkContext.getConf.get(s"spark.model.output_dir")

    val trainInput: DataFrame = spark.read.json(trainDir)

    val train: DataFrame = trainInput
      .withColumn("urls", col("visits.url"))
      .withColumn("url", explode(col("urls")))
      .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .groupBy("gender_age", "uid")
      .agg(collect_list("domain").alias("domains"))

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
      .fit(train)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val converter = new IndexToString()
      .setInputCol(lr.getPredictionCol)
      .setOutputCol("gender_age_prediction")
      .setLabels(indexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, converter))

    val model: PipelineModel = pipeline.fit(train)

    model.write.overwrite().save(modelDir)

  }
}
