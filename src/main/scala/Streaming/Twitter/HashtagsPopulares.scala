package Streaming.Twitter

import Streaming.Configuracao.{setupLogging, setupTwitter}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HashtagsPopulares {

  def main(args: Array[String]): Unit = {
    setupTwitter() // Configura as credenciais do Twitter utilizando o twitter.txt

    /**
      * Configurando o contexto do Spark Streaming:
      * Nome de "HashtagsPopulares"
      * Roda localmente utilizando todos os cores da CPU
      * Batches de dados de um segundo
      */
    val ssc = new StreamingContext("local[*]", "HashtagsPopulares", Seconds(1))

    setupLogging() // Livra-se do spam de log - é chamado após configurar o contexto.

    val tweets = TwitterUtils.createStream(ssc, None) // Cria um DStream do Twitter utilizando o contexto criado
    val statuses = tweets.map(status => status.getText) // Extrai o texto de cada atualização de status em RDDs utilizando o map

    // Cria um novo DStream transformando cada tweet em várias linhas, uma para cada palavra
    // o Map gera uma saída para cada entrada, o FlatMap tem um número de linhas da saída diferente do número na entrada
    val palavras = statuses.flatMap(tweet => tweet.split(" "))

    val hashtags = palavras.filter(palavra => palavra.startsWith("#")) // Filtramos para deixar só as palavras que começam com #
    // Mapeia cada hashtag para um par (chave, valor) = (hashtag, 1) para que possa ser contado de acordo com a hashtag
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))

    // Agora serão contadas as hashtags que foram escritas nos últimos 5 minutos, isso é feito utilizando o reduceByKeyAndWindow
    // O reduceByKeyAndWindow reduz os novos dados que entram na janela de tempo, e "inversamente reduzem" os que saem
    // Nesse caso, ele vai reduzir os últimos 5 minutos de dados a cada 1 segundo
    // Utilizamos esse método com pares (chave, valor)
    val contagemHashtag = hashtagKeyValues.reduceByKeyAndWindow((x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
    // Esse mesmo exemplo pode ser feito dessa outra forma:
    // val contagemHashtag = hashtagKeyValues.reduceByKeyAndWindow(_ + _, _ + _, Seconds(300), Seconds(1))

    // Agora ordenamos os resultados utilizando o segundo elemento da tupla, ou seja, o valor que se refere ao número de usos da hashtag
    val resultadoOrdenado = contagemHashtag.transform(rdd => rdd.sortBy(x => x._2, false))

    resultadoOrdenado.print() // Printa na tela os primeiros 10 colocados

    // Podemos escrever os resultados em um banco de dados, mas isso estará em outro exemplo
    // Definimos um diretório checkpoint e joga tudo nela
    ssc.checkpoint("/home/flaviodavim/Documentos/Scala")
    ssc.start()
    ssc.awaitTermination()
  }

}
