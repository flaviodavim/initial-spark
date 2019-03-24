package Streaming.Advanced

import Streaming.Configuracao.setupLogging
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LinearRegressionStreaming {

  def main(args: Array[String]): Unit = {

    /**
      * Configurando o contexto do Spark Streaming:
      * Nome de "LinearRegressionStreaming"
      * Roda localmente utilizando todos os cores da CPU
      * Batches de dados de um segundo
      */
    val ssc = new StreamingContext("local[*]", "LinearRegressionStreaming", Seconds(1))

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

    // Na porta 9999:
    // Vamos escutar pares de números onde a chave é o label que queremos prever e o valor é a feature utilizada para associar o valor que queremos prever
    // -> Importante perceber que o MLLib funciona melhor se o dado de entrada é normalizado, valores entre -1 e 1, com o zero como média
    // Na porta 7777:
    // Espera os dois, chave e valor, referentes ao label e ao feature
    // -> Na vida real provavelmente não teríamos o agrupamento com antecedência
    val linhasTreinamento = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val linhasTeste = ssc.socketTextStream("127.0.0.1", 7777, StorageLevel.MEMORY_AND_DISK_SER)


    // Nesse processo estamos transformando as strings em LabeledPoints
    // Esses objetos são esperado pela LinearRegression para o processamento
    val dadosTreinamento = linhasTreinamento.map(LabeledPoint.parse).cache()
    val dadosTeste = linhasTeste.map(LabeledPoint.parse)

    // Agora construímos o modelo de Linear Regression assim como os dados de treinamento são recebidos
    val numFeatures = 1
    val modelo = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.zeros(numFeatures))
    modelo.algorithm.setIntercept(true) // Necessário permitir o modelo ter uma interceptação em Y diferente de zero

    modelo.trainOn(dadosTreinamento) // Treinamos os dados utilizando o modelo criado

    // A medida que os dados de teste são recebidos, rodamos nosso modelo para prever os valor baseados na regressão linear
    // Ao utilizar o predictOnValues, temos a classificação correta dos labels, mas no mundo real não teremos esses dados
    // Quando não temos o valor esperado, utilizamos o predictOn
    modelo.predictOnValues(dadosTeste.map(lp => (lp.label.toInt, lp.features))).print()

    // Podemos escrever os resultados em um banco de dados, mas isso estará em outro exemplo
    // Definimos um diretório checkpoint e joga tudo nela
    ssc.checkpoint("/home/flaviodavim/Documentos/Scala")
    ssc.start()
    ssc.awaitTermination()
  }

}
