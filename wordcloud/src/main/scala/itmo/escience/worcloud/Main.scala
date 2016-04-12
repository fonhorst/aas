package itmo.escience.worcloud

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Created by user on 12.04.2016.
  */
object Main extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCloud")
    val sc = new SparkContext()

    val tweets = sc.textFile("file:///home/vagrant/wspace/data/isis_tiny.data")

    val wordCounts = tweets
      .map(JSON.parseFull)
      .map {
        case Some(tweet:Map[String, Any]) =>
          val text = tweet("text").asInstanceOf[String]
          text
      }
      .map(_.split(" "))
      .flatMap(_.map((_, 1)))
      .reduceByKey(_ + _)
      .sortBy { case (word, count) => count }
      //.map { case (word, count) => word }
      .take(10)

    println("---TOP 10 most frequent words ---")
    wordCounts.zipWithIndex.foreach {
      case ((word, count), i) => println(s"$i. $word $count")
    }
    println("---------------------------------")
  }
}
