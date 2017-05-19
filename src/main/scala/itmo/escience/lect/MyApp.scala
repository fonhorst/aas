package itmo.escience.lect

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection._

import scala.util.Random

object MyApp {

  def main(args: Array[String]): Unit = {

    val sc = initSpark()

    val users =
      sc.textFile("/mnt/share133/data-lect/Trump/users-subset.json")
        .filter(_.nonEmpty)
        .map(_.parseJson.convertTo[User](JsonFormats.userJsonFormat))
        .cache()


    val posts =
      sc.textFile("/mnt/share133/data-lect/Trump/users_posts-subset.json").cache()
      .filter(_.nonEmpty)
      .map(_.parseJson.convertTo[Post](JsonFormats.postJsonFormat))
      .cache()

    val posts2 =
      sc.textFile("/mnt/share133/data-lect/Trump/users_posts-subset-2.json").cache()
        .filter(_.nonEmpty)
        .map(_.parseJson.convertTo[Post](JsonFormats.postJsonFormat))
        .cache()

    calculateAveragePostSizePerUser(posts)

    println(users)
    println(posts)
  }

  private def initSpark(): SparkContext = {
    val master = "local[4]"
    var sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(s"SparkApp_${new Random().nextInt(10000)}")
      .set("spark.network.timeout", "6000s")
    val sc = new SparkContext(sparkConf)
    sc
  }

  private def calculateAveragePostSizePerUser(posts: RDD[Post]): Map[String, Double] = {
    posts
      .groupBy(_.userId)
      .map({
        case (userId, uposts) => (userId, uposts.map(_.text.length).sum.toDouble / uposts.size)
      })
      .collectAsMap()
  }

  private def calculateAveragePostSizePerUser(sc: SparkContext, posts: RDD[Post]): Map[String, Double] = {

    val cumulAvr = new CumulativePostLengthAverage()

    sc.register(cumulAvr)

    posts.foreach { p =>
      cumulAvr.add(p.userId -> p.text.length)
    }

    cumulAvr.value.map { case (uId, (sumlength, count)) => uId -> sumlength.toDouble / count}.toMap
  }

  private def broadcastingOfUsers(sc: SparkContext, users: RDD[User], posts: RDD[Post]): RDD[Post] = {

    val userNames = users.map(u => u._id -> u.key).distinct().collect().toMap

    val bUserNames = sc.broadcast(userNames)

    posts.map(p => Post( p._id, p.text, p.userId,
      Option(bUserNames.value.getOrElse(p.userId, null)))
    )
  }

  private def byJoiningWithUsers(sc: SparkContext, users: RDD[User], posts: RDD[Post]): RDD[Post] = {
    val mappedPosts = posts.map(p => p.userId -> p)
    val mappedUsers = users.map(u => u._id -> u)

    mappedPosts.join(mappedUsers).map { case (uId, (p, u)) => Post(p._id, p.text, p.userId, Option(u.key) )}
  }


}

case class User(_id: String, key: String)

case class Post(_id: String, text: String, userId: String, userName: Option[String] = None)

object JsonFormats {
  val userJsonFormat = UserJsonFormat
  val postJsonFormat = PostJsonFormat
}

object UserJsonFormat extends RootJsonFormat[User] {

  def write(c: User): JsValue = {
    throw new NotImplementedError()
  }

  def read(value: JsValue): User = {
    value.asJsObject.getFields("_id", "key") match {
      case Seq(JsString(id), JsString(key)) =>
        User(id, key)
      case _ => deserializationError("User expected")
    }
  }
}


object PostJsonFormat extends RootJsonFormat[Post] {

  def write(c: Post): JsValue = {
    throw new NotImplementedError()
  }

  def read(value: JsValue): Post = {
    value.asJsObject.getFields("_id", "text", "user") match {
      case Seq(JsString(id), JsString(text), user) =>
        val userId = user.asJsObject.getFields("id_str").head.toString()
        Post(id, text, userId)
      case _ => deserializationError("Post expected")
    }
  }
}