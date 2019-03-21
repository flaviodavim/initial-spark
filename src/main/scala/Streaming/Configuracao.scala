package Streaming

import java.util.regex.Pattern

object Configuracao {

  // Garante que apenas mensagens de erro serão logadas a fim de evitar spam
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  // Configura as credenciais do serviço do Twitter utilizando twitter.txt
  def setupTwitter(): Unit = {
    import scala.io.Source

    for(linha <- Source.fromFile("configuracoes-twitter/twitter.txt").getLines()) {
      val campos = linha.split(" ")
      if (campos.length == 2) {
        System.setProperty("twitter4j.oauth." + campos(0), campos(1))
      }
    }
  }

  // Recupera um padrão regex para analisar os logs de acesso do Apache
  def apacheLogPattern(): Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }

}