import org.apache.spark.sql.{DataFrame, SaveMode}
import java.nio.file.Paths

object FileManager {

  def saveCSV(df: DataFrame, fileName: String, outputPath: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(s"$outputPath/$fileName")
  }

  def readCSV(spark: org.apache.spark.sql.SparkSession, inputPath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)
  }

  def getOutputFolderName(inputPath: String): String = {
    val fileName = Paths.get(inputPath).getFileName.toString
    fileName.stripSuffix(".csv")
  }

  def toSnakeCase(str: String): String = {
    str.trim
      .replaceAll("[^a-zA-Z0-9]", "_") // Replace non-alphanumeric with _
      .replaceAll("([a-z])([A-Z])", "$1_$2") // camelCase to snake_case
      .replaceAll("_+", "_") // Collapse multiple underscores
      .stripPrefix("_")
      .stripSuffix("_")
      .toLowerCase
  }
}
