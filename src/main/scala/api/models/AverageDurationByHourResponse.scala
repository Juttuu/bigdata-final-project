package api.models

import spray.json.DefaultJsonProtocol

case class AverageDurationByHourResponse(
    pickup_hour: Int,
    average_duration_minutes: Double
)

object AverageDurationByHourResponse extends DefaultJsonProtocol {
  implicit val averageDurationByHourResponseFormat =
    jsonFormat2(AverageDurationByHourResponse.apply)
}
