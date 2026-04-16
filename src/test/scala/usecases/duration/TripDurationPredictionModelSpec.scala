package usecases.duration

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import support.SparkTestSession

class TripDurationPredictionModelSpec extends AnyFunSuite with SparkTestSession {
  import spark.implicits._

  test("prepareDataset keeps only valid duration rows and selected feature columns") {
    val featuredTrips = Seq(
      (2.5, 1, 12.0),
      (0.0, 1, 12.0),
      (2.5, 0, 12.0),
      (2.5, 1, 0.0),
      (70.0, 1, 30.0),
      (2.5, 1, 300.0)
    ).toDF("trip_distance", "passenger_count", "trip_duration_minutes")

    val prepared = TripDurationPredictionModel.prepareDataset(featuredTrips)

    assert(prepared.columns.toSeq === Seq("trip_distance", "passenger_count", "trip_duration_minutes"))
    assert(prepared.count() === 1)
    assert(prepared.filter(col("trip_distance") <= 0 || col("trip_distance") > 60.0).count() === 0)
    assert(prepared.filter(col("passenger_count") <= 0 || col("passenger_count") > 8).count() === 0)
    assert(prepared.filter(col("trip_duration_minutes") < 1.0 || col("trip_duration_minutes") > 180.0).count() === 0)
    assert(prepared.filter(col("trip_distance") === 2.5 && col("passenger_count") === 1 && col("trip_duration_minutes") === 12.0).count() === 1)
  }
}
