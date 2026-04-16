package api.routes

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import api.models.{AverageDurationByHourResponse, AverageFareByHourResponse, HotspotResponse}
import spray.json._
import usecases.analytics.AnalyticsQueryService

class AnalyticsRoutes(analyticsService: AnalyticsQueryService) extends SprayJsonSupport {
  import AverageDurationByHourResponse._
  import AverageFareByHourResponse._
  import HotspotResponse._
  import DefaultJsonProtocol._

  val routes: Route = pathPrefix("analytics") {
    path("hotspots") {
      get {
        parameter("limit".as[Int].?(10)) { limit =>
          if (limit <= 0) {
            completeJsonError("limit must be greater than 0")
          } else {
            complete(analyticsService.topPickupHotspotsResponse(limit))
          }
        }
      }
    } ~
      path("average-fare-by-hour") {
        get {
          complete(analyticsService.averageFareByHourResponse())
        }
      } ~
      path("average-duration-by-hour") {
        get {
          complete(analyticsService.averageDurationByHourResponse())
        }
      }
  }

  private def completeJsonError(message: String): Route = {
    val json = JsObject("error" -> JsString(message)).compactPrint
    complete(StatusCodes.BadRequest, HttpEntity(ContentTypes.`application/json`, json))
  }
}
