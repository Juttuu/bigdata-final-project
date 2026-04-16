package usecases.fare

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import utils.ProgressLogger

object FarePredictionService {
  def run(featuredTrips: DataFrame): Unit = {
    trainAndEvaluate(featuredTrips)
  }

  def trainAndEvaluate(featuredTrips: DataFrame): Option[PipelineModel] = {
    println("Starting fare prediction use case")
    val progress = ProgressLogger.start()

    progress.step(58, "Preparing fare training dataset")
    val fareDataset = FarePredictionModel.prepareDataset(featuredTrips)
    val rowCount = fareDataset.count()

    println(s"Fare prediction rows available: $rowCount")

    if (rowCount < 10) {
      println("Not enough rows to train a reliable fare prediction model.")
      return None
    }

    progress.step(62, "Splitting fare dataset into train and test sets")
    val Array(trainingData, testData) = fareDataset.randomSplit(Array(0.8, 0.2), seed = 42)

    val trainingCount = trainingData.count()
    val testCount = testData.count()

    println(s"Training rows: $trainingCount")
    println(s"Test rows: $testCount")

    if (trainingCount == 0 || testCount == 0) {
      println("Train/test split produced an empty dataset. Add more rows and try again.")
      return None
    }

    progress.step(68, "Training fare regression model")
    val model = FarePredictionModel.train(trainingData)
    progress.step(73, "Evaluating fare regression model")
    val predictions = FarePredictionModel.predict(model, testData)

    val rmse = FarePredictionModel.rmse(predictions)
    val r2 = FarePredictionModel.r2(predictions)

    println(f"Fare prediction RMSE: $rmse%.4f")
    println(f"Fare prediction R2: $r2%.4f")

    FarePredictionModel.linearRegressionModel(model).foreach { regressionModel =>
      println(f"Linear regression intercept: ${regressionModel.intercept}%.4f")
    }

    println("Sample fare predictions:")
    FarePredictionModel.samplePredictions(predictions).show(truncate = false)
    progress.step(76, "Fare model evaluation completed")

    Some(model)
  }

  def trainEvaluateAndSave(featuredTrips: DataFrame, outputPath: String): Option[PipelineModel] = {
    val evaluationModel = trainAndEvaluate(featuredTrips)

    evaluationModel.map { _ =>
      val progress = ProgressLogger.start()
      progress.step(77, "Retraining fare model on the full dataset for saving")
      val fullDataset = FarePredictionModel.prepareDataset(featuredTrips)
      val finalModel = FarePredictionModel.train(fullDataset)
      FarePredictionModel.save(finalModel, outputPath)
      progress.step(79, "Saved fare prediction model")
      println(s"Saved fare prediction model to: $outputPath")
      finalModel
    }
  }
}
