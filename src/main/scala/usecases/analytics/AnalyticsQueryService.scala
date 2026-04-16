package usecases.analytics

import api.models.{AverageDurationByHourResponse, AverageFareByHourResponse, HotspotResponse}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AnalyticsQueryService private (spark: SparkSession) {
  def printLocalSummary(): Unit = {
    println("Top pickup hotspots:")
    topPickupHotspots(limit = 10).show(truncate = false)

    println("Average fare by pickup hour:")
    averageFareByHour().show(truncate = false)

    println("Average trip duration by pickup hour:")
    averageDurationByHour().show(truncate = false)
  }

  def topPickupHotspots(limit: Int): DataFrame = {
    spark.sql(AnalyticsSqlQueries.topPickupHotspots(sanitizeLimit(limit)))
  }

  def averageFareByHour(): DataFrame = {
    spark.sql(AnalyticsSqlQueries.averageFareByHour)
  }

  def averageDurationByHour(): DataFrame = {
    spark.sql(AnalyticsSqlQueries.averageDurationByHour)
  }

  def topPickupHotspotsResponse(limit: Int): Seq[HotspotResponse] = {
    topPickupHotspots(limit).collect().map { row =>
      HotspotResponse(
        pickup_location_id = row.getAs[String]("pickup_location_id"),
        trip_count = row.getAs[Long]("trip_count")
      )
    }.toSeq
  }

  def averageFareByHourResponse(): Seq[AverageFareByHourResponse] = {
    averageFareByHour().collect().map { row =>
      AverageFareByHourResponse(
        pickup_hour = row.getAs[Int]("pickup_hour"),
        average_fare = row.getAs[Double]("average_fare")
      )
    }.toSeq
  }

  def averageDurationByHourResponse(): Seq[AverageDurationByHourResponse] = {
    averageDurationByHour().collect().map { row =>
      AverageDurationByHourResponse(
        pickup_hour = row.getAs[Int]("pickup_hour"),
        average_duration_minutes = row.getAs[Double]("average_duration_minutes")
      )
    }.toSeq
  }

  private def sanitizeLimit(limit: Int): Int = {
    math.max(1, math.min(limit, 100))
  }
}

object AnalyticsQueryService {
  def register(featuredTrips: DataFrame): AnalyticsQueryService = {
    featuredTrips.createOrReplaceTempView(AnalyticsSqlQueries.FeaturedTripsView)
    println(s"Registered Spark SQL temp view: ${AnalyticsSqlQueries.FeaturedTripsView}")
    new AnalyticsQueryService(featuredTrips.sparkSession)
  }
}
