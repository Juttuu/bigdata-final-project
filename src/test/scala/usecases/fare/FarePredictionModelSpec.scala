package usecases.fare

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import support.SparkTestSession

class FarePredictionModelSpec extends AnyFunSuite with SparkTestSession {
  import spark.implicits._

  test("prepareDataset keeps only valid fare rows and selected feature columns") {
    val featuredTrips = Seq(
      (2.5, 1, 9.0),
      (0.0, 1, 9.0),
      (2.5, 0, 9.0),
      (2.5, 1, 0.0),
      (70.0, 1, 30.0),
      (2.5, 1, 250.0)
    ).toDF("trip_distance", "passenger_count", "fare_amount")

    val prepared = FarePredictionModel.prepareDataset(featuredTrips)

    assert(prepared.columns.toSeq === Seq("trip_distance", "passenger_count", "fare_amount"))
    assert(prepared.count() === 1)
    assert(prepared.filter(col("trip_distance") <= 0 || col("trip_distance") > 60.0).count() === 0)
    assert(prepared.filter(col("passenger_count") <= 0 || col("passenger_count") > 8).count() === 0)
    assert(prepared.filter(col("fare_amount") < 2.5 || col("fare_amount") > 200.0).count() === 0)
    assert(prepared.filter(col("trip_distance") === 2.5 && col("passenger_count") === 1 && col("fare_amount") === 9.0).count() === 1)
  }
}
