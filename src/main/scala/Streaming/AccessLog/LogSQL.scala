package Streaming.AccessLog

import java.util.regex.Matcher

import Streaming.Configuracao.{apacheLogPattern, setupLogging}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Para rodar esse exemplo vamos utilizar o ncat para transmitir os dados para uma porta local do meu localhost
  * O ncat estará simulando um tipo de fonte de dados de log numa porta TCP
  * No linux executamos o seguinte comando:
  * $ nc -kl 9999 < access_log.txt
  */
object LogSQL {

  def main(args: Array[String]): Unit = {

    /**
      * Configurando o contexto do Spark Streaming:
      * Nome de "LogSQL"
      * Roda localmente utilizando todos os cores da CPU
      * Batches de dados de um segundo
      */
    val ssc = new StreamingContext("local[*]", "LogSQL", Seconds(1))

    setupLogging() // Livra-se do spam de log - é chamado após configurar o contexto.

    val pattern = apacheLogPattern() // Constrói  uma expressão regular para extrair os campos das linhas de log do apache

    // Cria um fluxo de entrada através de uma fonte TCP: host:porta
    // Os dados são recebidos utilizando um socket TCP que recebe bytes interpretados como UTF8 delimitando as linhas pelo /n
    // Storage level to use for storing the received objects (default: StorageLevel.MEMORY_AND_DISK_SER_2)
    // O storageLevel define a maneira como recebe-se e guarda-se os objetos que chegam
    // hostname: 127.0.0.1 -> Por ser o endereço local
    // port: 9999 -> A porta onde os dados estão chegando
    // storageLevel: StorageLevel.MEMORY_AND_DISK_SER
    // -> Guarda os RDDs como objetos Java serializado em um array de byte por partição
    // -> Partições que não cabem na memória são jogadas em disco ao invés de recomputá-las sempre que necessárias
    val linhas = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // Utiliza o padrão importado pelo apacheLogPattern() para separar as informações de cada linha
    // Depois de separadas, pegamos apenas o grupo que queremos, que é referente às requisições
    // Nesse exemplo vamos criar uma tupla para cada requisição referente a (URL, status, usuário agente)
    val requests = linhas.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val request = matcher.group(5)
        val requestFields = request.toString.split(" ")
        val url = util.Try(requestFields(1)) getOrElse "[erro]"
        (url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("erro", 0, "erro")
      }
    })

    // Essa etapa vai pegar de cada batch os RDDs que representam dados da janela atual
    requests.foreachRDD((rdd, time) => {

      // Pegamos a instância singleton do SQL Context
      // A partir dessa instância podemos fazer imports só quando necessário e em qualquer local do código
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      // SparkSQL pode criar DataFrames de classes Scala automaticamente, nesse caso temos a classe Registro
      // Transformamos cada tupla em um objeto "Registro" e em seguida convertemos o RDD em DataFrame usando o toDF()
      val requestsDataFrame = rdd.map(w => Registro(w._1, w._2, w._3)).toDF()

      // O createOrReplaceTempView é utilizado para criar uma visualização temporária do DataFrame
      // Essa visualização temporária permite que tratemos o DataFrame como uma tabela SQL
      requestsDataFrame.createOrReplaceTempView("requests")

      // Por termos criado essa visualização temporária podemos fazer consultas SQL nela utilizando o nome que havíamos definido
      // Nesse exemplo contamos o número de ocorrências de cada agente no RDD desse batch
      val contagemPalavrasDataFrame = sqlContext.sql("SELECT agente, count(*) AS total FROM requests GROUP BY agente")

      // Exibimos o resultado final
      println(s"========= $time =========")
      contagemPalavrasDataFrame.show()
    })

    // Podemos escrever os resultados em um banco de dados, mas isso estará em outro exemplo
    // Definimos um diretório checkpoint e joga tudo nela
    ssc.checkpoint("/home/flaviodavim/Documentos/Scala")
    ssc.start()
    ssc.awaitTermination()
  }

}

// Classe que utilizamos para converter o RDD em um DataFrame
case class Registro(url: String, status: Int, agente: String)

// Objeto utilizado para instanciar de forma Lazy o SQL Context de forma singleton
// Por ser singleton, há apenas uma instância da classe que mantem um ponto global de acesso
object SQLContextSingleton {
  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
