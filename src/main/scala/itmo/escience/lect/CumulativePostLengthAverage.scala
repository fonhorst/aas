package itmo.escience.lect

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import spray.json.{JsString, JsValue, RootJsonFormat, deserializationError}

import scala.collection.{Map, mutable}

import spray.json.DefaultJsonProtocol._
import spray.json._

/**
  * Created by nikolay on 19.05.17.
  */
class CumulativePostLengthAverage private (averPostLengthByUsers: mutable.HashMap[String, (Int, Int)])
  extends AccumulatorV2[(String, Int), mutable.HashMap[String, (Int, Int)]] {

  def this() = this(new mutable.HashMap[String, (Int, Int)]())

  override def isZero: Boolean = averPostLengthByUsers.isEmpty

  override def copy(): AccumulatorV2[(String, Int), mutable.HashMap[String, (Int, Int)]] =
    new CumulativePostLengthAverage(averPostLengthByUsers.clone())

  override def reset(): Unit = {
    averPostLengthByUsers.clear()
  }

  override def add(v: (String, Int)): Unit = {
    val (uId, length) = v
    val (sumlength, count) = averPostLengthByUsers.getOrElse(uId, (0, 0))
    averPostLengthByUsers.update(uId, (sumlength + length, count + 1))
  }

  override def merge(other: AccumulatorV2[(String, Int), mutable.HashMap[String, (Int, Int)]]): Unit = {
    for ((uId, (sumlength_other, count_other)) <- other.value) {
      val (sumlength, count) = averPostLengthByUsers.getOrElse(uId, (0, 0))
      averPostLengthByUsers.update(uId, (sumlength + sumlength_other, count + count_other))
    }
  }

  override def value: mutable.HashMap[String, (Int, Int)] = averPostLengthByUsers.clone()
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

object ProcessingFuncs {

  def loadUsersFromJson(path: String)(implicit sc: SparkContext): RDD[User] = {
      sc.textFile(path)
        .filter(_.nonEmpty)
        .map(_.parseJson.convertTo[User](JsonFormats.userJsonFormat))
  }

  def loadPostsFromJson(path: String)(implicit sc: SparkContext): RDD[Post] = {
    sc.textFile(path).cache()
      .filter(_.nonEmpty)
      .map(_.parseJson.convertTo[Post](JsonFormats.postJsonFormat))
  }

  def calculateAveragePostSizePerUser(posts: RDD[Post]): Map[String, Double] = {
    posts
      .groupBy(_.userId)
      .map({
        case (userId, uposts) => (userId, uposts.map(_.text.length).sum.toDouble / uposts.size)
      })
      .collectAsMap()
  }

  def calculateAveragePostSizePerUser(posts: RDD[Post])(implicit sc: SparkContext): Map[String, Double] = {

    val cumulAvr = new CumulativePostLengthAverage()

    sc.register(cumulAvr)

    posts.foreach { p =>
      cumulAvr.add(p.userId -> p.text.length)
    }

    cumulAvr.value.map { case (uId, (sumlength, count)) => uId -> sumlength.toDouble / count}.toMap
  }

  def broadcastingOfUsers(users: RDD[User], posts: RDD[Post])(implicit sc: SparkContext): RDD[Post] = {

    val userNames = users.map(u => u._id -> u.key).distinct().collect().toMap

    val bUserNames = sc.broadcast(userNames)

    posts.map(p => Post( p._id, p.text, p.userId,
      Option(bUserNames.value.getOrElse(p.userId, null)))
    )
  }

  def byJoiningWithUsers(users: RDD[User], posts: RDD[Post])(implicit sc: SparkContext): RDD[Post] = {
    val mappedPosts = posts.map(p => p.userId -> p)
    val mappedUsers = users.map(u => u._id -> u)

    mappedPosts.join(mappedUsers).map { case (uId, (p, u)) => Post(p._id, p.text, p.userId, Option(u.key) )}
  }

}