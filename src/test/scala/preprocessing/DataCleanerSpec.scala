package preprocessing

import org.scalatest.funsuite.AnyFunSuite
import support.SparkTestSession

class DataCleanerSpec extends AnyFunSuite with SparkTestSession {
  import spark.implicits._

  test("clean keeps only rows with valid coordinates, timestamps, distance, fare, and passenger count") {
    val rawTrips = Seq(
      (
        "2016-03-01 00:00:00",
        "2016-03-01 00:10:00",
        "1",
        "2.5",
        "-73.98",
        "40.76",
        "-74.00",
        "40.74",
        "9.0",
        "12.0"
      ),
      (
        "2016-03-01 00:00:00",
        "2016-03-01 00:10:00",
        "1",
        "0.0",
        "-73.98",
        "40.76",
        "-74.00",
        "40.74",
        "9.0",
        "12.0"
      ),
      (
        "2016-03-01 00:00:00",
        "2016-03-01 00:10:00",
        "0",
        "2.5",
        "-73.98",
        "40.76",
        "-74.00",
        "40.74",
        "9.0",
        "12.0"
      ),
      (
        "2016-03-01 00:10:00",
        "2016-03-01 00:00:00",
        "1",
        "2.5",
        "-73.98",
        "40.76",
        "-74.00",
        "40.74",
        "9.0",
        "12.0"
      ),
      (
        "2016-03-01 00:00:00",
        "2016-03-01 00:10:00",
        "1",
        "2.5",
        "0.0",
        "0.0",
        "-74.00",
        "40.74",
        "9.0",
        "12.0"
      )
    ).toDF(
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
      "passenger_count",
      "trip_distance",
      "pickup_longitude",
      "pickup_latitude",
      "dropoff_longitude",
      "dropoff_latitude",
      "fare_amount",
      "total_amount"
    )

    val cleaned = DataCleaner.clean(rawTrips)

    assert(cleaned.count() === 1)

    val row = cleaned.first()
    assert(row.getAs[Double]("trip_distance") === 2.5)
    assert(row.getAs[Int]("passenger_count") === 1)
    assert(row.getAs[Double]("fare_amount") === 9.0)
  }

  test("selectRelevantColumns accepts alternate schema names") {
    val rawTrips = Seq(
      (
        "2016-03-01 00:00:00",
        "2016-03-01 00:05:00",
        "2",
        "1.2",
        "-73.95",
        "40.78",
        "-73.96",
        "40.77",
        "7.5",
        "9.0"
      )
    ).toDF(
      "pickup_dt",
      "dropoff_dt",
      "passengers",
      "distance",
      "start_lon",
      "start_lat",
      "end_lon",
      "end_lat",
      "fare",
      "total"
    )

    val selected = DataCleaner.selectRelevantColumns(rawTrips)

    assert(selected.columns.contains("pickup_datetime"))
    assert(selected.columns.contains("dropoff_datetime"))
    assert(selected.columns.contains("trip_distance"))
    assert(selected.columns.contains("fare_amount"))
    assert(selected.columns.contains("total_amount"))
  }
}
