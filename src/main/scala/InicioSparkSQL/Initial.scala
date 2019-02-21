package InicioSparkSQL

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Initial {
  case class Pessoa(nome: String, idade: Long, salario: Long, beneficio: Long)

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Initial Spark").config("spark.master", "local").getOrCreate()

    val df = spark.read.json("./src/main/resources/pessoas.json")

    executarSelectBasico(spark)
    executarConsultaCustomizada(spark)
    executarConsultaComUDF(df)
    executarConsultaSql(df, spark)
    retiraLinhasComValoresNulos(df)

    spark.stop()
  }

  private def executarSelectBasico(spark: SparkSession): Unit = {
    val df = spark.read.json("./src/main/resources/pessoas.json")
    df.show(3)

    val adicionaUm = udf { x: Long => x + 1 }
  }

  private def executarConsultaCustomizada(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark.read.json("./src/main/resources/pessoas.json").as[Pessoa]

    df.withColumn("2030", df.col("idade").plus(11))
      .select($"nome", $"2030", $"salario", $"salario" + $"beneficio")
      .withColumnRenamed("salario", "Salário sem benefícios")
      .withColumnRenamed("(salario + beneficio)", "Salário com benefícios")
      .withColumnRenamed("2030", "Idade em 2030")
      //      .withColumn("add", 'idade + 10)
      .show()
  }

  private def executarConsultaComUDF(df: DataFrame): Unit = {
    val udfVezesDois = udf { x: Long => x * 2}
    df.withColumn("dobro", udfVezesDois(df.col("salario"))).select("nome", "salario", "dobro").show()
  }

  private def executarConsultaSql(df: DataFrame, spark: SparkSession): Unit = {
    df.createOrReplaceTempView("pessoas")
    df.show()

    val resultado = spark.sql("SELECT nome, salario FROM pessoas WHERE salario >= 5000")
    resultado.show()
  }

  private def retiraLinhasComValoresNulos(df: DataFrame): Unit = {
    // Retira todas as linhas que tem algum valor nulo
    df.na.drop().show()

    // Retira todas as linhas que tem mais de dois valores nulos
    df.na.drop(2).show()

    // Retira todas as linhas que tem o campo salário nulo
    df.na.drop(Array("salario")).show()

  }
}
