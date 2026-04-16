package preprocessing

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, IntegerType, TimestampType}
import utils.SchemaUtils

object DataCleaner {
  private val PickupDatetimeCandidates = Seq(
    "pickup_datetime",
    "tpep_pickup_datetime",
    "lpep_pickup_datetime",
    "pickup_dt"
  )

  private val DropoffDatetimeCandidates = Seq(
    "dropoff_datetime",
    "tpep_dropoff_datetime",
    "lpep_dropoff_datetime",
    "dropoff_dt"
  )

  private val PickupLocationCandidates = Seq(
    "pickup_location_id",
    "pulocationid",
    "pu_location_id",
    "pickup_taxizone_id"
  )

  private val DropoffLocationCandidates = Seq(
    "dropoff_location_id",
    "dolocationid",
    "do_location_id",
    "dropoff_taxizone_id"
  )

  private val TripDistanceCandidates = Seq("trip_distance", "distance")
  private val PassengerCountCandidates = Seq("passenger_count", "passengers")
  private val FareAmountCandidates = Seq("fare_amount", "fare")
  private val TotalAmountCandidates = Seq("total_amount", "total")
  private val PickupLongitudeCandidates = Seq("pickup_longitude", "start_lon")
  private val PickupLatitudeCandidates = Seq("pickup_latitude", "start_lat")
  private val DropoffLongitudeCandidates = Seq("dropoff_longitude", "end_lon")
  private val DropoffLatitudeCandidates = Seq("dropoff_latitude", "end_lat")

  def clean(rawTrips: DataFrame): DataFrame = {
    val selectedTrips = selectRelevantColumns(rawTrips)

    selectedTrips
      .filter(col("pickup_datetime").isNotNull)
      .filter(col("dropoff_datetime").isNotNull)
      .filter(hasPickupLocation && hasDropoffLocation)
      .filter(col("trip_distance").isNotNull && col("trip_distance") > 0)
      .filter(col("fare_amount").isNotNull && col("fare_amount") > 0)
      .filter(col("passenger_count").isNotNull)
      .filter(col("passenger_count") > 0 && col("passenger_count") <= 8)
      .filter(col("dropoff_datetime") > col("pickup_datetime"))
  }

  def selectRelevantColumns(rawTrips: DataFrame): DataFrame = {
    val pickupDatetime = SchemaUtils.requiredColumn(rawTrips, PickupDatetimeCandidates)
    val dropoffDatetime = SchemaUtils.requiredColumn(rawTrips, DropoffDatetimeCandidates)
    val pickupLocationId = SchemaUtils.optionalColumn(rawTrips, PickupLocationCandidates)
    val dropoffLocationId = SchemaUtils.optionalColumn(rawTrips, DropoffLocationCandidates)
    val pickupLongitude = SchemaUtils.optionalColumn(rawTrips, PickupLongitudeCandidates)
    val pickupLatitude = SchemaUtils.optionalColumn(rawTrips, PickupLatitudeCandidates)
    val dropoffLongitude = SchemaUtils.optionalColumn(rawTrips, DropoffLongitudeCandidates)
    val dropoffLatitude = SchemaUtils.optionalColumn(rawTrips, DropoffLatitudeCandidates)
    val tripDistance = SchemaUtils.requiredColumn(rawTrips, TripDistanceCandidates)
    val passengerCount = SchemaUtils.requiredColumn(rawTrips, PassengerCountCandidates)
    val fareAmount = SchemaUtils.requiredColumn(rawTrips, FareAmountCandidates)
    val totalAmount = SchemaUtils.optionalColumn(rawTrips, TotalAmountCandidates)

    requireLocationData(rawTrips, pickupLocationId, pickupLongitude, pickupLatitude, "pickup")
    requireLocationData(rawTrips, dropoffLocationId, dropoffLongitude, dropoffLatitude, "dropoff")

    rawTrips.select(
      col(pickupDatetime).cast(TimestampType).as("pickup_datetime"),
      col(dropoffDatetime).cast(TimestampType).as("dropoff_datetime"),
      optionalIntColumn(pickupLocationId, "pickup_location_id"),
      optionalIntColumn(dropoffLocationId, "dropoff_location_id"),
      optionalDoubleColumn(pickupLongitude, "pickup_longitude"),
      optionalDoubleColumn(pickupLatitude, "pickup_latitude"),
      optionalDoubleColumn(dropoffLongitude, "dropoff_longitude"),
      optionalDoubleColumn(dropoffLatitude, "dropoff_latitude"),
      col(tripDistance).cast(DoubleType).as("trip_distance"),
      col(passengerCount).cast(IntegerType).as("passenger_count"),
      col(fareAmount).cast(DoubleType).as("fare_amount"),
      optionalDoubleColumn(totalAmount, "total_amount")
    )
  }

  private def hasPickupLocation: Column = {
    col("pickup_location_id").isNotNull || validCoordinates("pickup_longitude", "pickup_latitude")
  }

  private def hasDropoffLocation: Column = {
    col("dropoff_location_id").isNotNull || validCoordinates("dropoff_longitude", "dropoff_latitude")
  }

  private def validCoordinates(longitudeColumn: String, latitudeColumn: String): Column = {
    col(longitudeColumn).isNotNull &&
      col(latitudeColumn).isNotNull &&
      col(longitudeColumn) =!= 0.0 &&
      col(latitudeColumn) =!= 0.0
  }

  private def optionalIntColumn(columnName: Option[String], outputName: String): Column = {
    columnName
      .map(name => col(name).cast(IntegerType).as(outputName))
      .getOrElse(lit(null).cast(IntegerType).as(outputName))
  }

  private def optionalDoubleColumn(columnName: Option[String], outputName: String): Column = {
    columnName
      .map(name => col(name).cast(DoubleType).as(outputName))
      .getOrElse(lit(null).cast(DoubleType).as(outputName))
  }

  private def requireLocationData(
      rawTrips: DataFrame,
      locationId: Option[String],
      longitude: Option[String],
      latitude: Option[String],
      locationType: String
  ): Unit = {
    val hasLocationId = locationId.isDefined
    val hasCoordinates = longitude.isDefined && latitude.isDefined

    if (!hasLocationId && !hasCoordinates) {
      val availableColumns = rawTrips.columns.mkString(", ")
      throw new IllegalArgumentException(
        s"Could not find $locationType location data. Expected a location ID or longitude/latitude columns. " +
          s"Available columns: $availableColumns"
      )
    }
  }
}
