package api.models

import spray.json.DefaultJsonProtocol

case class TripDurationPredictionResponse(predicted_duration_minutes: Double)

object TripDurationPredictionResponse extends DefaultJsonProtocol {
  implicit val tripDurationPredictionResponseFormat =
    jsonFormat1(TripDurationPredictionResponse.apply)
}
