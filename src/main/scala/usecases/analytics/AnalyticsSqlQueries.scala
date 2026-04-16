package usecases.analytics

object AnalyticsSqlQueries {
  val FeaturedTripsView = "featured_taxi_trips"

  def topPickupHotspots(limit: Int): String =
    s"""
       |SELECT
       |  COALESCE(
       |    CAST(pickup_location_id AS STRING),
       |    CONCAT(
       |      'coord:',
       |      CAST(ROUND(pickup_latitude, 3) AS STRING),
       |      ',',
       |      CAST(ROUND(pickup_longitude, 3) AS STRING)
       |    )
       |  ) AS pickup_location_id,
       |  COUNT(*) AS trip_count
       |FROM $FeaturedTripsView
       |WHERE pickup_location_id IS NOT NULL
       |   OR (pickup_latitude IS NOT NULL AND pickup_longitude IS NOT NULL)
       |GROUP BY
       |  COALESCE(
       |    CAST(pickup_location_id AS STRING),
       |    CONCAT(
       |      'coord:',
       |      CAST(ROUND(pickup_latitude, 3) AS STRING),
       |      ',',
       |      CAST(ROUND(pickup_longitude, 3) AS STRING)
       |    )
       |  )
       |ORDER BY trip_count DESC, pickup_location_id ASC
       |LIMIT $limit
       |""".stripMargin

  val averageFareByHour: String =
    s"""
       |SELECT
       |  pickup_hour,
       |  ROUND(AVG(fare_amount), 2) AS average_fare
       |FROM $FeaturedTripsView
       |WHERE pickup_hour IS NOT NULL
       |  AND fare_amount IS NOT NULL
       |GROUP BY pickup_hour
       |ORDER BY pickup_hour ASC
       |""".stripMargin

  val averageDurationByHour: String =
    s"""
       |SELECT
       |  pickup_hour,
       |  ROUND(AVG(trip_duration_minutes), 2) AS average_duration_minutes
       |FROM $FeaturedTripsView
       |WHERE pickup_hour IS NOT NULL
       |  AND trip_duration_minutes IS NOT NULL
       |GROUP BY pickup_hour
       |ORDER BY pickup_hour ASC
       |""".stripMargin
}
