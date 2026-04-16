package support

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkTestSession extends BeforeAndAfterAll { this: Suite =>
  protected lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .appName("nyc-taxi-test-session")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      if (!spark.sparkContext.isStopped) {
        spark.stop()
      }
    } finally {
      super.afterAll()
    }
  }
}
