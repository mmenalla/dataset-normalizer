import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class CategoricalCSVNormalizer(spark: SparkSession, databaseName: FilePaths, filePathsOpt: Option[FilePaths] = None) extends CSVNormalizer {

  def normalize(df: DataFrame): DataFrame = {
    var factDF = df
    var relations = Seq.empty[ColumnRelation]

    val categoricalThreshold = (0.7 * df.count()).toInt

    // Columns of string type and have low cardinality
    val candidateCols = df.schema.fields
      .filter(_.dataType == org.apache.spark.sql.types.StringType)
      .map(_.name)
      .filter { colName =>
        val distinctCount = df.select(col(colName)).na.drop().distinct().count()
        distinctCount > 1 && distinctCount <= categoricalThreshold
      }

    candidateCols.foreach { colName =>
      val lookupDF = df.select(colName).na.drop().distinct()
        .withColumn("id", monotonically_increasing_id())

      filePathsOpt.foreach(fp => FileManager.saveCSV(lookupDF, s"${colName}_lookup", fp.outputPath))

      PostgresManager.writeTable(lookupDF, s"${colName}_lookup", databaseName.outputPath)
      relations = relations :+ ColumnRelation(colName, s"${colName}_lookup")

      // Replace the original column in the fact table with the id
      factDF = factDF
        .join(lookupDF, Seq(colName), "left")
        .drop(colName)
        .withColumnRenamed("id", s"${colName}_id")
    }
    import spark.implicits._
    val relationsDF = relations.toDF("Column", "LookupTable")
    filePathsOpt.foreach(fp => FileManager.saveCSV(relationsDF, "relations", fp.outputPath))
    PostgresManager.writeTable(relationsDF, "relations", databaseName.outputPath)
    factDF
  }

  def save(df: DataFrame, fileName: String): Unit = {
    filePathsOpt.foreach(fp => FileManager.saveCSV(df, fileName, fp.outputPath))
    PostgresManager.writeTable(df, fileName, databaseName.outputPath)
  }
}
