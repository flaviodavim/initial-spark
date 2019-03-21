package Streaming.Twitter

import Streaming.Configuracao.{setupLogging, setupTwitter}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ExibirTweets {

  def main(args: Array[String]): Unit = {
    setupTwitter() // Configura as credenciais do Twitter utilizando o twitter.txt

    /**
      * Configurando o contexto do Spark Streaming:
      * Nome de "ExibirTweets"
      * Roda localmente utilizando todos os cores da CPU
      * Batches de dados de um segundo
      */
    val ssc = new StreamingContext("local[*]", "ExibirTweets", Seconds(1))

    setupLogging() // Livra-se do spam de log - é chamado após configurar o contexto.

    val tweets = TwitterUtils.createStream(ssc, None) // Cria um DStream do Twitter utilizando o contexto criado
    val statuses = tweets.map(status => status.getText) // Extrai o texto de cada atualização de status em RDDs utilizando o map

    statuses.print() // Exibe as primeiras dez atualizações

    ssc.start()
    ssc.awaitTermination()

  }

}
