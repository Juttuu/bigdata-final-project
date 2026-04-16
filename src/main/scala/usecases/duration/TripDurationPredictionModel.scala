package usecases.duration

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.{PipelineModel, PipelineStage}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object TripDurationPredictionModel {
  val LabelColumn = "trip_duration_minutes"
  val RawFeaturesColumn = "raw_features"
  val FeaturesColumn = "features"
  val PredictionColumn = "predicted_duration_minutes"

  val FeatureColumns: Seq[String] = Seq(
    "trip_distance",
    "passenger_count"
  )

  def prepareDataset(featuredTrips: DataFrame): DataFrame = {
    validateColumns(featuredTrips, FeatureColumns :+ LabelColumn)

    featuredTrips
      .select((FeatureColumns :+ LabelColumn).map(columnName => col(columnName)): _*)
      .na.drop(FeatureColumns :+ LabelColumn)
      .filter(col(LabelColumn) > 0)
      .filter(col(LabelColumn) >= 1.0 && col(LabelColumn) <= 180.0)
      .filter(col("trip_distance") > 0)
      .filter(col("trip_distance") <= 60.0)
      .filter(col("passenger_count") > 0 && col("passenger_count") <= 8)
  }

  def train(trainingData: DataFrame): PipelineModel = {
    val assembler = new VectorAssembler()
      .setInputCols(FeatureColumns.toArray)
      .setOutputCol(RawFeaturesColumn)
      .setHandleInvalid("skip")

    val scaler = new StandardScaler()
      .setInputCol(RawFeaturesColumn)
      .setOutputCol(FeaturesColumn)
      .setWithStd(true)
      .setWithMean(true)

    val regression = new LinearRegression()
      .setLabelCol(LabelColumn)
      .setFeaturesCol(FeaturesColumn)
      .setPredictionCol(PredictionColumn)
      .setMaxIter(100)
      .setRegParam(0.05)
      .setElasticNetParam(0.2)

    val stages: Array[PipelineStage] = Array(assembler, scaler, regression)

    new Pipeline()
      .setStages(stages)
      .fit(trainingData)
  }

  def predict(model: PipelineModel, data: DataFrame): DataFrame = {
    model.transform(data)
  }

  def save(model: PipelineModel, outputPath: String): Unit = {
    model.write.overwrite().save(outputPath)
  }

  def load(modelPath: String): PipelineModel = {
    PipelineModel.load(modelPath)
  }

  def rmse(predictions: DataFrame): Double = {
    regressionMetric(predictions, "rmse")
  }

  def r2(predictions: DataFrame): Double = {
    regressionMetric(predictions, "r2")
  }

  def samplePredictions(predictions: DataFrame, limit: Int = 10): DataFrame = {
    predictions.select(
      col(LabelColumn).as("actual_duration_minutes"),
      col(PredictionColumn),
      col("trip_distance"),
      col("passenger_count")
    ).limit(limit)
  }

  def linearRegressionModel(model: PipelineModel): Option[LinearRegressionModel] = {
    model.stages.collectFirst {
      case regressionModel: LinearRegressionModel => regressionModel
    }
  }

  private def regressionMetric(predictions: DataFrame, metricName: String): Double = {
    new RegressionEvaluator()
      .setLabelCol(LabelColumn)
      .setPredictionCol(PredictionColumn)
      .setMetricName(metricName)
      .evaluate(predictions)
  }

  private def validateColumns(dataFrame: DataFrame, requiredColumns: Seq[String]): Unit = {
    val availableColumns = dataFrame.columns.toSet
    val missingColumns = requiredColumns.filterNot(availableColumns.contains)

    if (missingColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"Missing columns for trip duration prediction: ${missingColumns.mkString(", ")}"
      )
    }
  }
}
