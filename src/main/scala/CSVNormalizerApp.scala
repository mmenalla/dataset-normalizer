import FileManager.toSnakeCase
import org.apache.spark.sql.SparkSession

object CSVNormalizerApp {

  def main(args: Array[String]): Unit = {
    val parsedArgs = parseArgs(args)

    val inputPath = parsedArgs.getOrElse("input", {
      println("Missing required argument: --input <file-path>")
      System.exit(1)
      ""
    })

    val saveAsFiles = parsedArgs.get("saveAsFiles").exists(_.toBoolean)

    val outputFolderName = FileManager.getOutputFolderName(inputPath)

    val filePaths = FilePaths(inputPath, s"output/$outputFolderName")
    val databaseName = FilePaths(inputPath, s"db_$outputFolderName")

    val spark = SparkSession.builder()
      .appName("CSVNormalizer")
      .master("local[*]")
      .getOrCreate()

    val rawDF = FileManager.readCSV(spark, inputPath)
    val df = rawDF.columns.foldLeft(rawDF) { (df, colName) =>
      df.withColumnRenamed(colName, toSnakeCase(colName))
    }

    val normalizer = new CategoricalCSVNormalizer(
      spark,
      databaseName,
      Some(filePaths).filter(_ => saveAsFiles)
    )

    val normalizedDF = normalizer.normalize(df)
    normalizer.save(normalizedDF, "fact_table")

    spark.stop()
  }

  def parseArgs(args: Array[String]): Map[String, String] = {
    args.sliding(2, 2).collect {
      case Array(key, value) if key.startsWith("--") => key.drop(2) -> value
    }.toMap
  }
}
