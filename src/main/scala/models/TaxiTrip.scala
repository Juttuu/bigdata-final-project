package models

import java.sql.Timestamp

case class TaxiTrip(
    pickupDatetime: Timestamp,
    dropoffDatetime: Timestamp,
    pickupLocationId: Option[Int],
    dropoffLocationId: Option[Int],
    pickupLongitude: Option[Double],
    pickupLatitude: Option[Double],
    dropoffLongitude: Option[Double],
    dropoffLatitude: Option[Double],
    tripDistance: Double,
    passengerCount: Int,
    fareAmount: Double,
    totalAmount: Option[Double]
)
