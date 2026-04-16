package features

import org.scalatest.funsuite.AnyFunSuite
import support.SparkTestSession

class FeatureEngineerSpec extends AnyFunSuite with SparkTestSession {
  import spark.implicits._

  test("addFeatures adds time features, weekend flag, and trip duration") {
    val cleanedTrips = Seq(
      (
        java.sql.Timestamp.valueOf("2016-03-05 14:00:00"),
        java.sql.Timestamp.valueOf("2016-03-05 14:30:00"),
        null.asInstanceOf[Integer],
        null.asInstanceOf[Integer],
        -73.98,
        40.76,
        -74.00,
        40.74,
        3.0,
        2,
        12.5,
        15.0
      )
    ).toDF(
      "pickup_datetime",
      "dropoff_datetime",
      "pickup_location_id",
      "dropoff_location_id",
      "pickup_longitude",
      "pickup_latitude",
      "dropoff_longitude",
      "dropoff_latitude",
      "trip_distance",
      "passenger_count",
      "fare_amount",
      "total_amount"
    )

    val featured = FeatureEngineer.addFeatures(cleanedTrips)
    val row = featured.first()

    assert(row.getAs[Int]("pickup_hour") === 14)
    assert(row.getAs[Int]("pickup_month") === 3)
    assert(row.getAs[Int]("is_weekend") === 1)
    assert(math.abs(row.getAs[Double]("trip_duration_minutes") - 30.0) < 0.001)
  }

  test("addFeatures removes rows with non-positive trip duration") {
    val cleanedTrips = Seq(
      (
        java.sql.Timestamp.valueOf("2016-03-01 10:00:00"),
        java.sql.Timestamp.valueOf("2016-03-01 10:00:00"),
        null.asInstanceOf[Integer],
        null.asInstanceOf[Integer],
        -73.98,
        40.76,
        -74.00,
        40.74,
        1.0,
        1,
        8.0,
        10.0
      )
    ).toDF(
      "pickup_datetime",
      "dropoff_datetime",
      "pickup_location_id",
      "dropoff_location_id",
      "pickup_longitude",
      "pickup_latitude",
      "dropoff_longitude",
      "dropoff_latitude",
      "trip_distance",
      "passenger_count",
      "fare_amount",
      "total_amount"
    )

    val featured = FeatureEngineer.addFeatures(cleanedTrips)

    assert(featured.count() === 0)
  }
}
