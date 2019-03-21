package Streaming.AccessLog

import java.util.regex.Matcher

import Streaming.Configuracao.{apacheLogPattern, setupLogging}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Para rodar esse exemplo vamos utilizar o ncat para transmitir os dados para uma porta local do meu localhost
  * O ncat estará simulando um tipo de fonte de dados de log numa porta TCP
  * No linux executamos o seguinte comando:
  * $ nc -kl 9999 < access_log.txt
  */
object AlarmeLog {

  def main(args: Array[String]): Unit = {

    /**
      * Configurando o contexto do Spark Streaming:
      * Nome de "AlarmeLog"
      * Roda localmente utilizando todos os cores da CPU
      * Batches de dados de um segundo
      */
    val ssc = new StreamingContext("local[*]", "AlarmeLog", Seconds(1))

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
    val statuses = linhas.map(x => {val matcher: Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(6) else "[error]" })

    // Transformamos os valores do DStream statuses em um valor inteiro, caso não seja possível, associamos o valor zero a ele
    // A partir desse código indentificamos se ele é de sucesso ou falha. Caso não seja nenhum dos dois o classificamos como "outro"
    val statusRequisicoes = statuses.map(x => {
      val codigoStatus = util.Try(x.toInt) getOrElse 0
      if (codigoStatus >= 200 && codigoStatus < 300) "Sucesso" else if (codigoStatus >= 500 && codigoStatus < 600) "Falha" else "Outro"
    })

    // Agora serão contadas os status dos últimos 5 minutos, isso é feito utilizando o countByValueAndWindow
    // O countByValueAndWindow retorna um DStream onde cada RDD é a contagem de elementos distintos da entrada
    // Nesse caso, ele vai reduzir os últimos 5 minutos de dados a cada 1 segundo
    // O último campo não é obrigatório, ele indica o número de partições de cada RDD no DStream
    val contagemStatus = statusRequisicoes.countByValueAndWindow(Seconds(300), Seconds(1))

    // Essa etapa vai pegar de cada batch os RDDs que representam dados da janela atual
    contagemStatus.foreachRDD((rdd, time) => {
      // Essas variáveis vão manter a informação de successo e erro de cada RDD
      var totalSucesso: Long = 0
      var totalErro: Long = 0

      if (rdd.count() > 0) {
        val elementos = rdd.collect()
        for (elemento <- elementos) {
          val resultado = elemento._1
          val contagem = elemento._2
          if (resultado == "Sucesso") totalSucesso += contagem
          if (resultado == "Falha") totalErro += contagem
        }
      }

      println("Total de erro: " + totalErro) // Exibe o total de casos de erro
      println("Total de sucesso: " + totalSucesso) // Exibe o total de casos de sucesso

      if (totalErro + totalSucesso > 100) {
        // A proporção utiliza o Try porque poderia haver a excessão de que a divisão fosse feita por zero
        val proporcao: Double = util.Try( totalErro.toDouble / totalSucesso.toDouble ) getOrElse 1.0
        // Utilizamos a proporção para identificar como lidar
        if (proporcao > 0.5) println("Acorde alguém! Algo está bem errado") else println("Todos os sistemas estão ok")
        // Em um caso real, utilizaríamos o JavaMail ou a biblioteca Scala de correio para mandar um e-mail
        // Na vida real esse não é um exemplo tão prático, pois seriam necessárias mais informações
      }
    })

    // Podemos escrever os resultados em um banco de dados, mas isso estará em outro exemplo
    // Definimos um diretório checkpoint e joga tudo nela
    ssc.checkpoint("/home/flaviodavim/Documentos/Scala")
    ssc.start()
    ssc.awaitTermination()

  }

}
