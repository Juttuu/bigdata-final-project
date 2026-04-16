package api.routes

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{MalformedRequestContentRejection, RejectionHandler, Route}
import api.models.{TripDurationPredictionRequest, TripDurationPredictionResponse}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import spray.json._
import usecases.duration.TripDurationPredictionModel

class TripDurationRoutes(model: PipelineModel, spark: SparkSession) extends SprayJsonSupport {
  import TripDurationPredictionRequest._
  import TripDurationPredictionResponse._
  import spark.implicits._

  private implicit val rejectionHandler: RejectionHandler = RejectionHandler
    .newBuilder()
    .handle {
      case MalformedRequestContentRejection(message, _) =>
        completeJsonError(StatusCodes.BadRequest, s"Invalid request JSON: $message")
    }
    .result()

  val routes: Route = handleRejections(rejectionHandler) {
    path("predict" / "duration") {
      post {
        entity(as[TripDurationPredictionRequest]) { request =>
          request.validationError match {
            case Some(errorMessage) =>
              completeJsonError(StatusCodes.BadRequest, errorMessage)
            case None =>
              complete(TripDurationPredictionResponse(predictDuration(request)))
          }
        }
      }
    }
  }

  private def predictDuration(request: TripDurationPredictionRequest): Double = {
    val requestData = Seq(
      (
        request.trip_distance,
        request.passenger_count,
        request.pickup_hour,
        request.pickup_day_of_week,
        request.pickup_month,
        request.is_weekend,
        request.pickup_longitude,
        request.pickup_latitude,
        request.dropoff_longitude,
        request.dropoff_latitude
      )
    ).toDF(
      "trip_distance",
      "passenger_count",
      "pickup_hour",
      "pickup_day_of_week",
      "pickup_month",
      "is_weekend",
      "pickup_longitude",
      "pickup_latitude",
      "dropoff_longitude",
      "dropoff_latitude"
    )

    TripDurationPredictionModel
      .predict(model, requestData)
      .select(TripDurationPredictionModel.PredictionColumn)
      .first()
      .getAs[Double](TripDurationPredictionModel.PredictionColumn)
  }

  private def completeJsonError(statusCode: StatusCodes.ClientError, message: String): Route = {
    val json = JsObject("error" -> JsString(message)).compactPrint
    complete(statusCode, HttpEntity(ContentTypes.`application/json`, json))
  }
}
