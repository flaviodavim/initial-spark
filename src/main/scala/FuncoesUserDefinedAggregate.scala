import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object FuncoesUserDefinedAggregate {

  object MinhaMedia extends UserDefinedAggregateFunction {
    // Esquema de linhas de entrada
    override def inputSchema: StructType = StructType(StructField("entrada", LongType) :: Nil)

    // Esquema de resultados intermediários
    override def bufferSchema: StructType = StructType(StructField("soma", LongType) :: StructField("contagem", LongType) :: Nil)

    // O tipo do resultado final
    override def dataType: DataType = DoubleType

    // Determina se a mesma entrada sempre produz o mesmo resultado
    override def deterministic: Boolean = true

    // É chamado uma vez por grupo. É a condição inicial
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    // Chamado novamente para cada registro de entrada
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    // Usado para computar resultados parciais e combiná-los juntos
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // Usado para computar o resultado final
    override def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Trabalhando com User Defined Aggregate Functions").config("spark.master", "local").getOrCreate()
    val df = spark.read.json("src/main/resources/pessoas.json")
    df.createOrReplaceTempView("pessoas")

    spark.udf.register("minhaMedia", MinhaMedia)
    val resultado = spark.sql("SELECT minhaMedia(salario) as media_salarial FROM pessoas")
    resultado.show()

  }
}
