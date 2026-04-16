package io

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoader {
  def load(spark: SparkSession, inputPath: String): DataFrame = {
    val lowerPath = inputPath.toLowerCase

    if (lowerPath.endsWith(".parquet")) {
      loadParquet(spark, inputPath)
    } else if (lowerPath.endsWith(".csv") || lowerPath.endsWith(".csv.gz")) {
      loadCsv(spark, inputPath)
    } else {
      throw new IllegalArgumentException(
        s"Unsupported file type for path: $inputPath. Please use CSV or Parquet."
      )
    }
  }

  private def loadCsv(spark: SparkSession, inputPath: String): DataFrame = {
    // Keep CSV loading fast and predictable for large raw files.
    // DataCleaner casts only the columns this project needs.
    spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(inputPath)
  }

  private def loadParquet(spark: SparkSession, inputPath: String): DataFrame = {
    spark.read.parquet(inputPath)
  }
}
