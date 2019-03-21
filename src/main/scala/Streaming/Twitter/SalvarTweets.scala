package Streaming.Twitter

import Streaming.Configuracao.{setupLogging, setupTwitter}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SalvarTweets {

  def main(args: Array[String]): Unit = {
    setupTwitter() // Configura as credenciais do Twitter utilizando o twitter.txt

    /**
      * Configurando o contexto do Spark Streaming:
      * Nome de "SalvarTweets"
      * Roda localmente utilizando todos os cores da CPU
      * Batches de dados de um segundo
      */
    val ssc = new StreamingContext("local[*]", "SalvarTweets", Seconds(1))

    setupLogging() // Livra-se do spam de log - é chamado após configurar o contexto.

    val tweets = TwitterUtils.createStream(ssc, None) // Cria um DStream do Twitter utilizando o contexto criado
    val statuses = tweets.map(status => status.getText) // Extrai o texto de cada atualização de status em RDDs utilizando o map

//    statuses.saveAsTextFiles("Tweets", "txt")
//    Aqui é uma forma de descarregar cada partição de todos os fluxos para arquivos individuais
//    Mas fazer isso de maneira mais detalhada é melhor para ter um pouco mais de controle

//    Vamos contar quantos tweets foram recebidos para para automaticamente a fim de não lotar o disco
    var totalTweets: Long = 0

    statuses.foreachRDD((rdd, time) => {
      if (rdd.count() > 0) { // Só processa RDDs com dados
        val repaticionadoRDD = rdd.repartition(1).cache() // Combina os resultados de cada partição em único RDD
        repaticionadoRDD.saveAsTextFile("Tweets_" + time.milliseconds.toString) // Cria um diretório com os resultados
        totalTweets += repaticionadoRDD.count()
        println("Número de tweets: " + totalTweets)
        if (totalTweets > 1000) {
          System.exit(0) // Para de executar quando o número de tweets for maior que 1000
        }
      }
    })

    // Podemos escrever os resultados em um banco de dados, mas isso estará em outro exemplo
    // Definimos um diretório checkpoint e joga tudo nela
    ssc.checkpoint("/home/flaviodavim/Documentos/Scala")
    ssc.start()
    ssc.awaitTermination()
  }

}
