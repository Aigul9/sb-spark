import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}


object users_items {
  def main(args: Array[String]): Unit = {

    def expr(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit(0).as(x)
      })
    }

//    def setDir(dir: String): String = {
//      if (dir.startsWith("/")) dir else "/users/aigul.sibgatullina/" + dir
//    }

    val spark = SparkSession.builder()
      .config("spark.sql.session.timeZone", "UTC")
      .appName("lab05")
      .getOrCreate()

//    val inputDir: String = setDir(spark.sparkContext.getConf.get(s"spark.users_items.input_dir"))
//    val outputDir: String = setDir(spark.sparkContext.getConf.get(s"spark.users_items.output_dir"))
    val inputDir: String = spark.sparkContext.getConf.get(s"spark.users_items.input_dir")
    val outputDir: String = spark.sparkContext.getConf.get(s"spark.users_items.output_dir")
    val update: Integer = spark.sparkContext.getConf.get("spark.users_items.update", "1").toInt

    val hdfsView = s"$inputDir/view/"
    val hdfsBuy = s"$inputDir/buy/"

    val view = spark.read.json(hdfsView)
    val buy = spark.read.json(hdfsBuy)

    val resView = view
      .where(col("uid").isNotNull)
      .select(col("uid"), concat(lit("view_"), regexp_replace(lower(col("item_id")), "[- ]", "_")).alias("item_id_cleaned"))
      .groupBy("uid")
      .pivot("item_id_cleaned")
      .count()

    val resBuy = buy
//      .where(col("uid").isNotNull)
      .select(col("uid"), concat(lit("buy_"), regexp_replace(lower(col("item_id")), "[- ]", "_")).alias("item_id_cleaned"))
      .groupBy("uid")
      .pivot("item_id_cleaned")
      .count()

    val finalRes = resBuy.join(resView, Seq("uid"), "outer").na.fill(0)

    val viewMaxDate = view.select(max(col("date"))).first() {
      0
    }.toString
    val buyMaxDate = buy.select(max(col("date"))).first() {
      0
    }.toString
    val maxDate = if (viewMaxDate > buyMaxDate) viewMaxDate else buyMaxDate

    if (update == 0) {

      finalRes
        .write
        .mode("overwrite")
        .parquet(s"$outputDir/$maxDate")
    } else {

      val inputDF = spark
        .read
        .option("inferSchema", "true")
        .parquet(s"$outputDir/20200429")

      val cols1 = resView.columns.toSet
      val cols2 = resBuy.columns.toSet
      val cols = cols1 ++ cols2

      val unionDF = inputDF
        .select(expr(cols1, cols):_*)
        .union(finalRes.select(expr(cols2, cols):_*))
        .na.fill(0)
        .groupBy("uid")
        .sum()

      val renamedDF = unionDF.columns.foldLeft(unionDF) { (df, columnName) =>
        df.withColumnRenamed(columnName, columnName.replace("sum(", "").replace(")", ""))
      }

      renamedDF
        .write
        .mode("overwrite")
        .parquet(s"$outputDir/$maxDate")
    }
  }
}
