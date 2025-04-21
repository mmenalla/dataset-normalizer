import org.apache.spark.sql.DataFrame

trait CSVNormalizer {
  def normalize(df: DataFrame): DataFrame
  def save(df: DataFrame, fileName: String): Unit
}
