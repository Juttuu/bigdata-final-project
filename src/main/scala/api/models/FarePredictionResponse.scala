package api.models

import spray.json.DefaultJsonProtocol

case class FarePredictionResponse(predicted_fare: Double)

object FarePredictionResponse extends DefaultJsonProtocol {
  implicit val farePredictionResponseFormat = jsonFormat1(FarePredictionResponse.apply)
}
