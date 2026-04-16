package usecases.duration

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import utils.ProgressLogger

object TripDurationPredictionService {
  def run(featuredTrips: DataFrame): Unit = {
    trainAndEvaluate(featuredTrips)
  }

  def trainAndEvaluate(featuredTrips: DataFrame): Option[PipelineModel] = {
    println("Starting trip duration prediction use case")
    val progress = ProgressLogger.start()

    progress.step(82, "Preparing trip duration training dataset")
    val durationDataset = TripDurationPredictionModel.prepareDataset(featuredTrips)
    val rowCount = durationDataset.count()

    println(s"Trip duration prediction rows available: $rowCount")

    if (rowCount < 10) {
      println("Not enough rows to train a reliable trip duration prediction model.")
      return None
    }

    progress.step(86, "Splitting trip duration dataset into train and test sets")
    val Array(trainingData, testData) = durationDataset.randomSplit(Array(0.8, 0.2), seed = 42)

    val trainingCount = trainingData.count()
    val testCount = testData.count()

    println(s"Duration training rows: $trainingCount")
    println(s"Duration test rows: $testCount")

    if (trainingCount == 0 || testCount == 0) {
      println("Train/test split produced an empty duration dataset. Add more rows and try again.")
      return None
    }

    progress.step(90, "Training trip duration regression model")
    val model = TripDurationPredictionModel.train(trainingData)
    progress.step(95, "Evaluating trip duration regression model")
    val predictions = TripDurationPredictionModel.predict(model, testData)

    val rmse = TripDurationPredictionModel.rmse(predictions)
    val r2 = TripDurationPredictionModel.r2(predictions)

    println(f"Trip duration prediction RMSE: $rmse%.4f")
    println(f"Trip duration prediction R2: $r2%.4f")

    TripDurationPredictionModel.linearRegressionModel(model).foreach { regressionModel =>
      println(f"Trip duration linear regression intercept: ${regressionModel.intercept}%.4f")
    }

    println("Sample trip duration predictions:")
    TripDurationPredictionModel.samplePredictions(predictions).show(truncate = false)
    progress.step(97, "Trip duration model evaluation completed")

    Some(model)
  }

  def trainEvaluateAndSave(featuredTrips: DataFrame, outputPath: String): Option[PipelineModel] = {
    val evaluationModel = trainAndEvaluate(featuredTrips)

    evaluationModel.map { _ =>
      val progress = ProgressLogger.start()
      progress.step(98, "Retraining trip duration model on the full dataset for saving")
      val fullDataset = TripDurationPredictionModel.prepareDataset(featuredTrips)
      val finalModel = TripDurationPredictionModel.train(fullDataset)
      TripDurationPredictionModel.save(finalModel, outputPath)
      progress.step(99, "Saved trip duration prediction model")
      println(s"Saved trip duration prediction model to: $outputPath")
      finalModel
    }
  }
}
