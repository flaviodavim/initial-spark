package Streaming.Advanced

import Streaming.Configuracao._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KMeansStreaming {

  def main(args: Array[String]): Unit = {

    /**
      * Configurando o contexto do Spark Streaming:
      * Nome de "GerenciamentoSessoes"
      * Roda localmente utilizando todos os cores da CPU
      * Batches de dados de um segundo
      */
    val ssc = new StreamingContext("local[*]", "KMeansStreaming", Seconds(1))

    setupLogging()

    // Cria dois fluxos de entrada através de fontes TCP: host:porta
    // Os dados são recebidos utilizando um socket TCP que recebe bytes interpretados como UTF8 delimitando as linhas pelo /n
    // Storage level to use for storing the received objects (default: StorageLevel.MEMORY_AND_DISK_SER_2)
    // O storageLevel define a maneira como recebe-se e guarda-se os objetos que chegam
    // hostname: 127.0.0.1 -> Por ser o endereço local
    // port: 7777/9999 -> A porta onde os dados estão chegando
    // storageLevel: StorageLevel.MEMORY_AND_DISK_SER
    // -> Guarda os RDDs como objetos Java serializado em um array de byte por partição
    // -> Partições que não cabem na memória são jogadas em disco ao invés de recomputá-las sempre que necessárias

    // Na porta 9999 vamos escutar dados no formato [renda, idade] para o treinamento
    // Na porta 7777 vamos escutar dados no formato (ID do cluster, [renda, idade])
    // -> Na vida real provavelmente não teríamos o agrupamento com antecedência
    val linhasTreinamento = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val linhasTeste = ssc.socketTextStream("127.0.0.1", 7777, StorageLevel.MEMORY_AND_DISK_SER)


    // Nesse processo estamos transformando as strings em Vectors, para dados de treinamento, e LabeledPoints, para dados de teste
    // Esses objetos são esperado pelo KMeans para o processamento
    val dadosTreinamento = linhasTreinamento.map(Vectors.parse).cache()
    val dadosTeste = linhasTeste.map(LabeledPoint.parse)

    dadosTreinamento.print() // Exibe uma parte dos dados que chegam para serem treinados, para entender como o dado é recebido

    // Aqui construímos um modelo de agrupamento para cinco grupos e duas características, idade e renda
    val modelo = new StreamingKMeans().setK(5).setDecayFactor(1.0).setRandomCenters(2, 0.0)

    modelo.trainOn(dadosTreinamento) // Treinamos os dados utilizando o modelo criado


    // Aqui recebemos os dados de teste, continuando a refinar o modelo de agrupamento e exibindo os resultados
    // No mundo real, só usaríamos o predictOn que espera receber os dados de entrada já que não saberíamos o agrupamento através do tempo
    // Mas nesse caso imprimimos os IDs do cluster ao lado das previsões
    // Os ID's podem variar, mas o agrupamento deve se manter consistente independente do local
    modelo.predictOnValues(dadosTeste.map(lp => (lp.label.toInt, lp.features))).print()

    // Podemos escrever os resultados em um banco de dados, mas isso estará em outro exemplo
    // Definimos um diretório checkpoint e joga tudo nela
    ssc.checkpoint("/home/flaviodavim/Documentos/Scala")
    ssc.start()
    ssc.awaitTermination()
  }

}
