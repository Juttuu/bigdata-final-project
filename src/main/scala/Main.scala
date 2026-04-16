import api.ApiServer
import config.SparkSessionFactory
import features.FeatureEngineer
import io.DataLoader
import preprocessing.DataCleaner
import usecases.analytics.AnalyticsQueryService
import usecases.duration.TripDurationPredictionModel
import usecases.duration.TripDurationPredictionService
import usecases.fare.FarePredictionModel
import usecases.fare.FarePredictionService
import utils.ProgressLogger

object Main {
  private val DefaultModelDirectory = "trained-models"
  private val TrainCommand = "train"
  private val ApiCommand = "api"
  private val ApiAnalyticsCommand = "api-analytics"

  def main(args: Array[String]): Unit = {
    val appName = "NYC Taxi Analytics and ML"
    val spark = SparkSessionFactory.create(appName)

    try {
      println(s"Starting $appName")

      val command = args.headOption.getOrElse {
        printUsage()
        return
      }

      command match {
        case TrainCommand =>
          val inputPath = args.lift(1).getOrElse {
            printUsage()
            return
          }
          val modelDirectory = args.lift(2).getOrElse(DefaultModelDirectory)
          println(s"Input dataset path: $inputPath")
          println(s"Model directory: $modelDirectory")
          runTraining(inputPath, modelDirectory)
        case ApiCommand =>
          val modelDirectory = args.lift(1).getOrElse(DefaultModelDirectory)
          println(s"Model directory: $modelDirectory")
          runApi(modelDirectory)
        case ApiAnalyticsCommand =>
          val inputPath = args.lift(1).getOrElse {
            printUsage()
            return
          }
          val modelDirectory = args.lift(2).getOrElse(DefaultModelDirectory)
          println(s"Input dataset path: $inputPath")
          println(s"Model directory: $modelDirectory")
          runApiWithAnalytics(inputPath, modelDirectory)
        case unknownCommand =>
          println(s"Unknown command: $unknownCommand")
          printUsage()
      }
    } finally {
      spark.stop()
      println("Spark session stopped")
    }

    def loadFeaturedTrips(inputPath: String) = {
      val progress = ProgressLogger.start()
      progress.step(5, "Loading dataset")
      val rawTrips = DataLoader.load(spark, inputPath)

      progress.step(12, "Inspecting raw dataset")
      println("Raw dataset schema:")
      rawTrips.printSchema()

      println("Raw dataset sample:")
      rawTrips.show(5, truncate = false)

      val rawCount = rawTrips.count()
      println(s"Rows before cleaning: $rawCount")

      progress.step(25, "Cleaning dataset")
      val cleanedTrips = DataCleaner.clean(rawTrips)
      val cleanedCount = cleanedTrips.count()

      println(s"Rows after cleaning: $cleanedCount")

      println("Cleaned dataset schema:")
      cleanedTrips.printSchema()

      println("Cleaned dataset sample:")
      cleanedTrips.show(10, truncate = false)

      progress.step(40, "Engineering features")
      val featuredTrips = FeatureEngineer.addFeatures(cleanedTrips)
      featuredTrips.cache()
      val featuredCount = featuredTrips.count()

      println("Featured dataset schema:")
      featuredTrips.printSchema()

      println("Featured dataset sample:")
      featuredTrips.show(10, truncate = false)

      progress.step(50, s"Featured dataset ready with $featuredCount rows")

      featuredTrips
    }

    def runTraining(inputPath: String, modelDirectory: String): Unit = {
      val progress = ProgressLogger.start()
      progress.step(0, "Training pipeline started")
      val featuredTrips = loadFeaturedTrips(inputPath)

      progress.step(55, "Training fare prediction model")
      FarePredictionService.trainEvaluateAndSave(featuredTrips, fareModelPath(modelDirectory))
      progress.step(80, "Training trip duration prediction model")
      TripDurationPredictionService.trainEvaluateAndSave(featuredTrips, durationModelPath(modelDirectory))
      featuredTrips.unpersist()
      progress.done("Training completed and models saved")
    }

    def runApi(modelDirectory: String): Unit = {
      val fareModel = FarePredictionModel.load(fareModelPath(modelDirectory))
      val durationModel = TripDurationPredictionModel.load(durationModelPath(modelDirectory))

      println(s"Loaded fare prediction model from: ${fareModelPath(modelDirectory)}")
      println(s"Loaded trip duration prediction model from: ${durationModelPath(modelDirectory)}")

      ApiServer.start(fareModel, durationModel, analyticsService = None, spark)
    }

    def runApiWithAnalytics(inputPath: String, modelDirectory: String): Unit = {
      val progress = ProgressLogger.start()
      progress.step(0, "Starting API with analytics")
      val featuredTrips = loadFeaturedTrips(inputPath)
      progress.step(60, "Registering Spark SQL analytics view")
      val analyticsService = AnalyticsQueryService.register(featuredTrips)
      analyticsService.printLocalSummary()

      progress.step(75, "Loading saved prediction models")
      val fareModel = FarePredictionModel.load(fareModelPath(modelDirectory))
      val durationModel = TripDurationPredictionModel.load(durationModelPath(modelDirectory))

      println(s"Loaded fare prediction model from: ${fareModelPath(modelDirectory)}")
      println(s"Loaded trip duration prediction model from: ${durationModelPath(modelDirectory)}")

      progress.done("API and analytics are ready")
      ApiServer.start(fareModel, durationModel, Some(analyticsService), spark)
    }

    def fareModelPath(modelDirectory: String): String = {
      s"$modelDirectory/fare-prediction"
    }

    def durationModelPath(modelDirectory: String): String = {
      s"$modelDirectory/trip-duration-prediction"
    }

    def printUsage(): Unit = {
      println("Usage:")
      println("""  sbt "run train <dataset-path> [model-directory]"""")
      println("""  sbt "run api [model-directory]"""")
      println("""  sbt "run api-analytics <dataset-path> [model-directory]"""")
      println("")
      println("Examples:")
      println("""  sbt "run train data/yellow_tripdata_2016-03.csv"""")
      println("""  sbt "run api"""")
      println("""  sbt "run api-analytics data/yellow_tripdata_2016-03.csv"""")
    }
  }
}
