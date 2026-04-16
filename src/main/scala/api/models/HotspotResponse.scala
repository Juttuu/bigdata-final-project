package api.models

import spray.json.DefaultJsonProtocol

case class HotspotResponse(
    pickup_location_id: String,
    trip_count: Long
)

object HotspotResponse extends DefaultJsonProtocol {
  implicit val hotspotResponseFormat = jsonFormat2(HotspotResponse.apply)
}
