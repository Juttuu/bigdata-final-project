package api.models

import spray.json.DefaultJsonProtocol

case class AverageFareByHourResponse(
    pickup_hour: Int,
    average_fare: Double
)

object AverageFareByHourResponse extends DefaultJsonProtocol {
  implicit val averageFareByHourResponseFormat = jsonFormat2(AverageFareByHourResponse.apply)
}
