package api

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import api.routes.{AnalyticsRoutes, FareRoutes, TripDurationRoutes}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import usecases.analytics.AnalyticsQueryService

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ApiServer {
  def start(
      fareModel: PipelineModel,
      durationModel: PipelineModel,
      analyticsService: Option[AnalyticsQueryService],
      spark: SparkSession
  ): Unit = {
    implicit val system: ActorSystem = ActorSystem("nyc-taxi-api")
    implicit val executionContext = system.dispatcher

    val host = "localhost"
    val port = 8080
    val fareRoutes = new FareRoutes(fareModel, spark)
    val durationRoutes = new TripDurationRoutes(durationModel, spark)
    val predictionRoutes = fareRoutes.routes ~ durationRoutes.routes
    val routes = analyticsService
      .map(service => predictionRoutes ~ new AnalyticsRoutes(service).routes)
      .getOrElse(predictionRoutes)

    val binding = Http()
      .newServerAt(host, port)
      .bind(routes)

    binding.foreach { serverBinding =>
      val address = serverBinding.localAddress
      println(s"API server running at http://${address.getHostString}:${address.getPort}")
      println("Try prediction endpoints, or press Ctrl+C to stop the server.")
      if (analyticsService.isDefined) {
        println("Analytics endpoints are also enabled.")
      }
    }

    Await.result(system.whenTerminated, Duration.Inf)
  }
}
