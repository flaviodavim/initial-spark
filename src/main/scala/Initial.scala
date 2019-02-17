import org.apache.spark.sql.SparkSession

object Initial {
  case class Pessoa(nome: String, idade: Long)

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Initial Spark").config("spark.master", "local").getOrCreate()

    runBasicDataFrameExample(spark)

    spark.stop()
  }

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark.read.json("./src/main/resources/pessoas.json").as[Pessoa]
    df.show()
  }
}
