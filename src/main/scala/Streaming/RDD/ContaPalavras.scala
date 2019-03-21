package Streaming.RDD

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Cria um RDD de linha do arquivo de texto e continua contando a medida que as palavras aparecem
  */
object ContaPalavras {

  def main(args: Array[String]): Unit = {
    // Prepara um SparkContext chamado WordCount que roda localmente utilizando todos os cores disponíveis
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    val input = sparkContext.textFile("src/main/resources/livro-exemplo.txt") // Cria um RDD de linhas do livro
    val palavras = input.flatMap(line => line.split(' ')) // Usa o flatMap para converter em um RDD de cada palavra em cada linha

    val palavrasMinusculas = palavras.map(palavra => palavra.toLowerCase()) // Converte as palavras em minúsculas
    val wordCounts = palavrasMinusculas.countByValue() // Conta as ocorrências de cada palavra única

    val sample = wordCounts.take(20)
    for((word, count) <- sample) { println(word + " " + count) } // Exibe os 20 primeiros resultados

    sparkContext.stop()
  }

}
