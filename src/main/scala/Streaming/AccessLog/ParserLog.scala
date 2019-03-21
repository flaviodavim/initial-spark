package Streaming.AccessLog
import java.util.regex.Matcher

import Streaming.Configuracao._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Para rodar esse exemplo vamos utilizar o ncat para transmitir os dados para uma porta local do meu localhost
  * O ncat estará simulando um tipo de fonte de dados de log numa porta TCP
  * No linux executamos o seguinte comando:
  * $ nc -kl 9999 < access_log.txt
  */
object ParserLog {

  def main(args: Array[String]): Unit = {

    /**
    * Configurando o contexto do Spark Streaming:
      * Nome de "LogParser"
    * Roda localmente utilizando todos os cores da CPU
    * Batches de dados de um segundo
    */
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))

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
    val requests = linhas.map(x => {val matcher: Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5) })

    // De cada requisição criamos um novo DStream só com as URLs
    val urls = requests.map(x => {val arr = x.toString.split(" "); if (arr.size == 3) arr(1) else "[error]"})


    // Agora serão contadas as urls acessadas nos últimos 5 minutos, isso é feito utilizando o reduceByKeyAndWindow
    // O reduceByKeyAndWindow reduz os novos dados que entram na janela de tempo, e "inversamente reduzem" os que saem
    // Nesse caso, ele vai reduzir os últimos 5 minutos de dados a cada 1 segundo
    // Utilizamos esse método com pares (chave, valor), então utiliza-se o map antes para formatar o DStream da maneira correta
    val contagemUrl = urls.map(x => (x, 1)).reduceByKeyAndWindow((x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
    // Esse mesmo exemplo pode ser feito dessa outra forma:
    // val contagemHashtag = hashtagKeyValues.reduceByKeyAndWindow(_ + _, _ + _, Seconds(300), Seconds(1))

    // Agora ordenamos os resultados utilizando o segundo elemento da tupla, ou seja, o valor que se refere ao número de acessos
    val resultadosOrdenados = contagemUrl.transform(rdd => rdd.sortBy(x => x._2, false))
    resultadosOrdenados.print()

    // Podemos escrever os resultados em um banco de dados, mas isso estará em outro exemplo
    // Definimos um diretório checkpoint e joga tudo nela
    ssc.checkpoint("/home/flaviodavim/Documentos/Scala")
    ssc.start()
    ssc.awaitTermination()
  }

}
