package Streaming.AccessLog

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.{Matcher, Pattern}

import Streaming.Configuracao._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.window

object StructuredStreaming {

  case class Log(ip: String, client: String, user: String, dateTime: String, request: String, status: String, bytes: String, referer: String, agent: String)

  val logPattern: Pattern = apacheLogPattern()
  val datePattern: Pattern = Pattern.compile("\\[(.*?) .+]")

  // Converte o tempo de log do Apache em um formato esperado pelo Spark/SQL
  def ParseCampoData(campo: String): Option[String] = {
    val matcherData = datePattern.matcher(campo)
    if (matcherData.find) {
      val stringData = matcherData.group(1)
      val formatoData = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val data = formatoData.parse(stringData)

      val timestamp = new Timestamp(data.getTime)
      Option(timestamp.toString)
    } else {
      None
    }
  }

  // Esse método será responsável por converter uma linha "crua" em um objeto Log ou None, caso a linha esteja corrompida
  def parseLog(x: Row): Option[Log] = {
    val matcher: Matcher = logPattern.matcher(x.getString(0))
    if (matcher.matches()) {
      // O Some serve para tentar criar o objeto Log, caso algo dê errado ele não gera uma excessão
      // O matcher associa cada grupo a um parâmtro da classe Log
      Some(Log(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        ParseCampoData(matcher.group(4)).getOrElse(""),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)
      ))
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {

    // Com StructuredStreaming a configuração do Spark Streaming é um pouco diferente
    /**
      * Configurando o contexto do Spark Streaming:
      * Nome de "StructuredStreaming"
      * Roda localmente utilizando todos os cores da CPU
      * Definimos o local do checkpoint
      * -> É o local onde o sistema vai escrever toda a informação de checkpoint
      * -> Em caso de falha ou desligamento é possível recuperar o progresso e o status de uma query antiga utilizando o que tem no local de checkpoint
      *  getOrCreate vai criar uma nova sessão apenas se não houver dados no local de checkpoint de onde ele possa inicializar
      *  -> Importante para quando houver uma falha, permitindo que a sessão reinicie de onde parou
      */
    val spark = SparkSession.builder()
      .appName("StructuredStreaming")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "/home/flaviodavim/Documentos/Scala")
      .getOrCreate()

    setupLogging() // Livra-se do spam de log - é chamado após configurar o contexto.

    // Carrega arquivos de texto de um diretório específicado e retorna um DataFrame
    // O DataFrame inicia com uma coluna de Strings chamada "value" e seguida de colunas particionadas, caso existam
    // Por padrão, cada linha do arquivo é uma linha do DataFrame
    val dadoCru = spark.readStream.text("src/main/scala/Streaming/AccessLog/logs")

    import spark.implicits._

    // o Map gera uma saída para cada entrada, o FlatMap pode ter um número de linhas da saída diferente do número na entrada
    // Para cada linha do DataFrame utilizamos o método parseLog para transformá-lo em linhas de objetos Log
    // Depois de transformar as linhas em objeto Log, selecionamos apenas duas colunas/variáveis: status e datetime
    val dadosEstruturados = dadoCru.flatMap(parseLog).select("status", "datetime")

    // Tratamos os dados estruturados para agrupá-los por status numa janela de tempo especificada
    // A janela de período dos dados é definida pelo "window" onde passamos o campo usado como referência e o tempo da janela
    val janela = dadosEstruturados.groupBy($"status", window($"datetime", "1 hour")).count().orderBy("window")

    // O writeStream é uma interface para salvar o conteúdo do DataSet streaming em um armazenamento externo
    // O outputMode especifica como o dado vai ser escrito nesse armazenamento. Existem três formas:
    // -> 1. append: apenas as novas linhas do DataSet são escritas
    // -> 2. complete: todas as linhas do DataSet são escritas sempre que há uma nova atualização
    // -> 3. update: apenas as linhas que foram atualizadas no DataSet são escritas quando houver atualização
    // O format especifica a fonte de dados de saída, nesse caso será o próprio console
    val query = janela.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination() // Ficará rodando até que paremos a execução

    spark.stop()
  }
}