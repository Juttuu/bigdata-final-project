package utils

import org.apache.spark.sql.DataFrame

object SchemaUtils {
  def requiredColumn(dataFrame: DataFrame, candidates: Seq[String]): String = {
    findColumn(dataFrame, candidates).getOrElse {
      val availableColumns = dataFrame.columns.mkString(", ")
      val expectedColumns = candidates.mkString(", ")

      throw new IllegalArgumentException(
        s"Could not find required column. Expected one of: $expectedColumns. " +
          s"Available columns: $availableColumns"
      )
    }
  }

  def optionalColumn(dataFrame: DataFrame, candidates: Seq[String]): Option[String] = {
    findColumn(dataFrame, candidates)
  }

  private def findColumn(dataFrame: DataFrame, candidates: Seq[String]): Option[String] = {
    val columnsByNormalizedName = dataFrame.columns.map { columnName =>
      normalize(columnName) -> columnName
    }.toMap

    candidates
      .map(normalize)
      .collectFirst {
        case normalizedCandidate if columnsByNormalizedName.contains(normalizedCandidate) =>
          columnsByNormalizedName(normalizedCandidate)
      }
  }

  private def normalize(columnName: String): String = {
    columnName.toLowerCase.replaceAll("[^a-z0-9]", "")
  }
}
