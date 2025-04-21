import java.sql.DriverManager
import org.apache.spark.sql.{DataFrame, SaveMode}

object PostgresManager {

  private val host = "localhost"
  private val port = 5432
  private val database = "postgres"
  private val user = "root"
  private val password = "password"
  private val driver = "org.postgresql.Driver"

  private def jdbcUrl(schema: String): String = {
    s"jdbc:postgresql://$host:$port/$database?currentSchema=$schema"
  }

  private def ensureSchemaExists(schema: String): Unit = {
    val conn = DriverManager.getConnection(
      s"jdbc:postgresql://$host:$port/$database", user, password
    )
    try {
      val stmt = conn.createStatement()
      stmt.executeUpdate(s"CREATE SCHEMA IF NOT EXISTS \"$schema\"")
      println(s"Schema '$schema' is ready.")
    } finally {
      conn.close()
    }
  }

  def writeTable(df: DataFrame, tableName: String, schema: String): Unit = {
    ensureSchemaExists(schema)
    df.write
      .format("jdbc")
      .option("url", jdbcUrl(schema))
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .option("driver", driver)
      .mode(SaveMode.Overwrite)
      .save()

    println(s"Wrote table '$schema.$tableName' to PostgreSQL.")
  }

  def writeMultipleTables(tables: Map[String, DataFrame], schema: String): Unit = {
    ensureSchemaExists(schema)
    tables.foreach { case (tableName, df) =>
      writeTable(df, tableName, schema)
    }
  }
}
