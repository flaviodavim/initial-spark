package Streaming.Twitter

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import Streaming.Configuracao.{setupLogging, setupTwitter}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MediaTamanhoTweets {

  def main(args: Array[String]): Unit = {

    setupTwitter() // Configura as credenciais do Twitter utilizando o twitter.txt

    /**
      * Configurando o contexto do Spark Streaming:
      * Nome de "MediaTamanhoTweets"
      * Roda localmente utilizando todos os cores da CPU
      * Batches de dados de um segundo
      */
    val ssc = new StreamingContext("local[*]", "MediaTamanhoTweets", Seconds(1))

    setupLogging() // Livra-se do spam de log - é chamado após configurar o contexto.

    val tweets = TwitterUtils.createStream(ssc, None) // Cria um DStream do Twitter utilizando o contexto criado
    val statuses = tweets.map(status => status.getText) // Extrai o texto de cada atualização de status em RDDs utilizando o map

    val tamanho = statuses.map(status => status.length()) // Mapeia cada status no número de caracteres de cada um

    // Como existem múltiplos processos mexendo nessas variáveis ao mesmo templo, utilizamos a classe AtomicLong
    // O AtomicLong garante que esses contadores são thread-safe
    val totalTweets = new AtomicLong(0)
    val totalCaracteres = new AtomicLong(0)
    var maiorTweet = new AtomicInteger(0)

    // Em versões  Spark 1.6+ devemos olhar também para o método mapWithState que permite de maneira segura e eficiente
    // de seguir o estado global com pares de chave e valor. Existirão outros exemplo desse

    tamanho.foreachRDD((rdd, tempo) => {
      var contagem = rdd.count()
      if (contagem > 0) { // Só adiciona se existirem valores (linhas) no RDD
        totalTweets.getAndAdd(contagem) // Adiciona ao totalTweets o número de linhas (tweets) do rdd
        // Reduzimos o RDD pegando cada linha, onde cada uma representa o tamanho de um tweet, e somando elas
        // Em seguida somamos esse valor ao totalCaracteres
        totalCaracteres.getAndAdd(rdd.reduce((x, y) => x + y))
        val maximoValorRDD = rdd.max()
        if (maximoValorRDD > maiorTweet.get()) {
          maiorTweet.set(maximoValorRDD)
        }

        println("Número total de tweets: " + totalTweets.get())
        println("Número total de caracteres: " + totalCaracteres.get())
        println("Média de caracteres por tweet: " + (totalCaracteres.get() / totalTweets.get()))
        println("Maior tweet: " + maiorTweet.get())
      }

    })

    // Podemos escrever os resultados em um banco de dados, mas isso estará em outro exemplo
    // Definimos um diretório checkpoint e joga tudo nela
    ssc.checkpoint("/home/flaviodavim/Documentos/Scala")
    ssc.start()
    ssc.awaitTermination()
  }

}
