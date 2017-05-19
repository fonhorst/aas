package itmo.escience.lect

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Random

object MyApp {

  def main(args: Array[String]): Unit = {

    val spark = initSpark()
    implicit val sc = spark.sparkContext

    val users = ProcessingFuncs.loadUsersFromJson("/mnt/share133/data-lect/Trump/users-subset.json").cache()

    val posts = ProcessingFuncs.loadPostsFromJson("/mnt/share133/data-lect/Trump/users_posts-subset.json").cache()

    println(users.count())
    println(posts.count())

  }

  private def initSpark(): SparkSession = {
    val master = "local[4]"
    var sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(s"SparkApp_${new Random().nextInt(10000)}")
      .set("spark.network.timeout", "6000s")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    spark
  }
}