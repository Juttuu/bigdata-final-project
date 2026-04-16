package features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, dayofweek, hour, month, unix_timestamp, when}

object FeatureEngineer {
  def addFeatures(cleanedTrips: DataFrame): DataFrame = {
    val withDuration = addTripDuration(cleanedTrips)

    withDuration
      .filter(col("trip_duration_minutes").isNotNull)
      .filter(col("trip_duration_minutes") > 0)
      .withColumn("pickup_hour", hour(col("pickup_datetime")))
      .withColumn("pickup_day_of_week", dayofweek(col("pickup_datetime")))
      .withColumn("pickup_month", month(col("pickup_datetime")))
      .withColumn("is_weekend", when(col("pickup_day_of_week").isin(1, 7), 1).otherwise(0))
      .select(
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pickup_location_id"),
        col("dropoff_location_id"),
        col("pickup_longitude"),
        col("pickup_latitude"),
        col("dropoff_longitude"),
        col("dropoff_latitude"),
        col("trip_distance"),
        col("passenger_count"),
        col("fare_amount"),
        col("total_amount"),
        col("pickup_hour"),
        col("pickup_day_of_week"),
        col("pickup_month"),
        col("is_weekend"),
        col("trip_duration_minutes")
      )
  }

  private def addTripDuration(cleanedTrips: DataFrame): DataFrame = {
    // Convert timestamps to seconds first, then divide by 60.0 to keep fractional minutes.
    cleanedTrips.withColumn(
      "trip_duration_minutes",
      (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0
    )
  }
}
