package Streaming.Advanced

import java.util.regex.Matcher

import Streaming.Configuracao._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

/**
  * Muitos processamentos de fluxo devem manter o estado através de um período de tempo.
  * Nesse exemplo, um dos casos mais comuns, para entender o comportamento do usuário em um site, devemos manter a informação da sessão do usuário
  * Isso é feito persistindo e continuamente atualizando o estado baseado nas ações do usuário
  * Existem duas formas de fazer isso: updateStateByKey e mapWithState
  * O mapWithState foi feita com suporte para padrões comuns que exigiam condificação manual e otimização no updateStateByKey, assim tem uma performance 10x melhor
  *
  * https://databricks.com/blog/2016/02/01/faster-stateful-stream-processing-in-apache-spark-streaming.html
  */

object GerenciamentoSessoes {
  // O Spark Streamingo tem uma API simples para processar fluxos de estado
  // O desenvolvedor só especifica a estrutura do estado e a lóǵica para atulizá-lo
  // O Spark Streamingo cuida da distribuição do estado através do cluster, sua manutenção, restauração transparente de
  // falhas e uma garantia de tolerância a falha end-to-end
  //



  // Essa é uma classe casual, que define rapidamente um tipo complexo que contém:
  // -> Tamanho da sessão
  // -> Lista de URLs visitadas
  // Essa variáveis compõe os dados de sessão que queremos preservar através de uma sessão especificada
  // O "case class" cria automaticamente o construtor e acessos que usaremos
  case class DadoSessao(var tamanhoSessao: Long, var fluxoClicks: List[String])

  // Esse método será chamada sempre que um novo dado chega no fluxo e mantém o estado através de qualquer chave que seja definida. Nesse caso o método espera:
  // -> Endereço IP como a chave
  // -> Uma string referente a URL (encapsulada em uma Option para lidar com excessões)
  // -> SessionData atualizada que leva essa nova linha em conta
  def funcaoRastrearEstado(tempoBatch: Time, ip: String, url: Option[String], estado: State[DadoSessao]): Option[(String, DadoSessao)] = {
    // Extraímos o estado anterior, utilizamos o "getOption" para retirar o valor e caso ele não exista temos o getOrElse para lidar com excessões
    // Caso não exista um estado anterior, vamos criar um novo estado que é representado pelo objeto "DadoSessao"
    val estadoAnterior = estado.getOption.getOrElse(DadoSessao(0, List()))

    // Criar um novo estado que incrementa o tamanha da sessão em um, adiciona a URL para o "fluxoClicks" e segura a lista em 19 itens
    val estadoNovo = DadoSessao(estadoAnterior.tamanhoSessao + 1L, (estadoAnterior.fluxoClicks :+ url.getOrElse("empty")).take(10))

    estado.update(estadoNovo) // Aqui atualizamos o estado com o novo valor que definimos

    Some((ip, estadoNovo)) // Retorna um resultado que é par entre IP e estadoNovo, onde o IP é a chave e o estadoValor é o valor
  }

  def main(args: Array[String]): Unit = {
    /**
      * Configurando o contexto do Spark Streaming:
      * Nome de "GerenciamentoSessoes"
      * Roda localmente utilizando todos os cores da CPU
      * Batches de dados de um segundo
      */
    val ssc = new StreamingContext("local[*]", "GerenciamentoSessoes", Seconds(1))

    setupLogging() // Livra-se do spam de log - é chamado após configurar o contexto.

    val pattern = apacheLogPattern() // Constrói  uma expressão regular para extrair os campos das linhas de log do apache

    // O StateSpec é usado para especificar os parâmetros de uma transformação utilizando o mapWithState
    val estadoEsperado = StateSpec.function(funcaoRastrearEstado _).timeout(Minutes(30))

    // Cria um fluxo de entrada através de uma fonte TCP: host:porta
    // Os dados são recebidos utilizando um socket TCP que recebe bytes interpretados como UTF8 delimitando as linhas pelo /n
    // Storage level to use for storing the received objects (default: StorageLevel.MEMORY_AND_DISK_SER_2)
    // O storageLevel define a maneira como recebe-se e guarda-se os objetos que chegam
    // hostname: 127.0.0.1 -> Por ser o endereço local
    // port: 9999 -> A porta onde os dados estão chegando
    // storageLevel: StorageLevel.MEMORY_AND_DISK_SER
    // -> Guarda os RDDs como objetos Java serializado em um array de byte por partição
    // -> Partições que não cabem na memória são jogadas em disco ao invés de recomputá-las sempre que necessárias
    val linhas = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK)

    // Nesse processamento, transformamos o DStream de linhas em um que é composto por (ip, url)
    val requests = linhas.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val ip = matcher.group(1)
        val camposRequests = matcher.group(5).toString.split(" ")
        val url = scala.util.Try(camposRequests(1)) getOrElse "[error]"
        (ip, url )
      } else {
        ("error", "error")
      }
    })

    // Agora vamos criar o novo DStream utilizando o StateSpec para atualizar o estado do dado
    // Notamos que o RDD requests tem o par chave/valor de IP/URL, exatamente o que o trackStateFunc espera receber de entrada
    val requestsComEstado = requests.mapWithState(estadoEsperado)

    // Aqui tiramos pegamos um snapshot do estado atual para podermos olhar para ele
    val snapshotFluxoEstados = requestsComEstado.stateSnapshots()

    // Aqui processamos cada RDD de cada batch que chega
    // Aqui vamos expor os dados de estado com o SparkSQL, mas no mundo real o comum seria atualizar um banco externo
    snapshotFluxoEstados.foreachRDD((rdd, time) => {
      // Pegamos a instância singleton do SQL Context
      // A partir dessa instância podemos fazer imports só quando necessário e em qualquer local do código
      val sqlContext = SQLContextSingleton.getInstance(rdd.context)
      import sqlContext.implicits._

      // A sintaxe é um pouco diferente do último exemplo de SparkSQL
      // toDF pode pegar uma lista de nomes de colunas e se o número de colunas combinarem com o que existe no RDD,
      // ela funciona sem uma classe intermediária para definir os registros
      // Nosso RDD contém pares de chave/valor onde a chave é o endereço IP e o valor é um objeto DadoSessao
      // O RDD é a saída do trackStateFunc

      val requestsDataFrame = rdd.map(x => (x._1, x._2.tamanhoSessao, x._2.fluxoClicks)).toDF("ip", "tamanhoSessao", "fluxoClicks")

      // O createOrReplaceTempView é utilizado para criar uma visualização temporária do DataFrame
      // Essa visualização temporária permite que tratemos o DataFrame como uma tabela SQL
      requestsDataFrame.createOrReplaceTempView("sessionData")

      val sessoesDataFrame = sqlContext.sql("select * from sessionData")
      println(s"========= $time =========")
      sessoesDataFrame.show()
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
