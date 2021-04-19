import akka.actor.FSM.->
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils
import java.io._
import scala.io.Source


class Statistics() {
  var counter = 0
  var LangMap = Map("en" -> 0, "fr" -> 0, "pl" -> 0, "de" -> 0, "es" -> 0, "it" -> 0)


  def incrementCounter(): Unit = {
    counter += 1
    println(counter)
  }

  def countByCountry(name: String): Unit = {
    val bufferedSource = Source.fromFile(name)
    for (line <- bufferedSource.getLines){
      if (line != ""){
        counter += 1
        if (LangMap.exists({ case (key, _) => key == line })) {
          LangMap.foreach {
            case (key, value) =>
              if (key == line) {
                LangMap -= key
                LangMap += (key -> (value + 1))
              }
          }
        }
      }
    }
    bufferedSource.close
  }

  def printStats(): Unit = {
    LangMap.foreach{
      case (key, value) =>
        println(key + " " + (100.*(value.toDouble)/counter).toString + " %")
    }
    println("Number of filtered Tweets: " + counter)
  }
}


object twitter_retrieval {

  def retrieve(writer: BufferedWriter, filters : Array[String], run_time : Int): Unit = {

    val appName = "Twitter Streaming WUST"
    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val consumerKey = ""
    val consumerSecret = ""
    val accessToken = ""
    val accessTokenSecret = ""

    val cb = new ConfigurationBuilder

    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(cb.build)
    val tweets = TwitterUtils.createStream(ssc, Some(auth), filters)

    tweets.foreachRDD{
      x => {
        val my_array = x.map(j => { j.getLang }).collect()
        for (i <- my_array.indices){
          val my_string = my_array(i)
          for (j <- my_string.indices){
            writer.write(my_string(j))
            if ( (j % 2) == 1){  // we put newline
              writer.newLine()
            }
          }
          if ( (i % 2) == 1){  // we put newline
            writer.newLine()
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(run_time)
  }


  def main(args: Array[String]) {
    val writer = new BufferedWriter(new FileWriter("temp_countries.txt"))
    val filters = Array("BlackLivesMatter")
    retrieve(writer, filters, 100000)
    writer.close()

    val stats = new Statistics()
    stats.countByCountry("temp_countries.txt")
    stats.printStats()

  }

}
