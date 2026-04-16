package api.models

import spray.json.DefaultJsonProtocol

case class TripDurationPredictionRequest(
    pickup_longitude: Double,
    pickup_latitude: Double,
    dropoff_longitude: Double,
    dropoff_latitude: Double,
    trip_distance: Double,
    passenger_count: Int,
    pickup_hour: Int,
    pickup_day_of_week: Int,
    pickup_month: Int,
    is_weekend: Int
) {
  def validationError: Option[String] = {
    if (!isValidLongitude(pickup_longitude)) {
      Some("pickup_longitude must be between -180 and 180 and cannot be 0")
    } else if (!isValidLatitude(pickup_latitude)) {
      Some("pickup_latitude must be between -90 and 90 and cannot be 0")
    } else if (!isValidLongitude(dropoff_longitude)) {
      Some("dropoff_longitude must be between -180 and 180 and cannot be 0")
    } else if (!isValidLatitude(dropoff_latitude)) {
      Some("dropoff_latitude must be between -90 and 90 and cannot be 0")
    } else if (trip_distance <= 0) {
      Some("trip_distance must be greater than 0")
    } else if (passenger_count <= 0 || passenger_count > 8) {
      Some("passenger_count must be between 1 and 8")
    } else if (pickup_hour < 0 || pickup_hour > 23) {
      Some("pickup_hour must be between 0 and 23")
    } else if (pickup_day_of_week < 1 || pickup_day_of_week > 7) {
      Some("pickup_day_of_week must be between 1 and 7")
    } else if (pickup_month < 1 || pickup_month > 12) {
      Some("pickup_month must be between 1 and 12")
    } else if (is_weekend != 0 && is_weekend != 1) {
      Some("is_weekend must be 0 or 1")
    } else {
      None
    }
  }

  private def isValidLongitude(value: Double): Boolean = {
    value >= -180 && value <= 180 && value != 0.0
  }

  private def isValidLatitude(value: Double): Boolean = {
    value >= -90 && value <= 90 && value != 0.0
  }
}

object TripDurationPredictionRequest extends DefaultJsonProtocol {
  implicit val tripDurationPredictionRequestFormat =
    jsonFormat10(TripDurationPredictionRequest.apply)
}
