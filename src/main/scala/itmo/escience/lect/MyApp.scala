package itmo.escience.lect

import org.apache.spark.{SparkConf, SparkContext}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.Random

object MyApp {

  def main(args: Array[String]): Unit = {

    val sc = initSpark()

    val users =
      sc.textFile("/mnt/share133/data-lect/Trump/users_posts-subset.json")
        .filter(_.nonEmpty)
        .map(_.parseJson.convertTo[User](JsonFormats.userJsonFormat)).take(10)


    val posts =
      sc.textFile("/mnt/share133/data-lect/Trump/users_posts-subset.json")
      .filter(_.nonEmpty)
      .map(_.parseJson.convertTo[Post](JsonFormats.postJsonFormat)).take(10)

    println(users)
    println(posts)
  }

  def initSpark(): SparkContext = {
    val master = "local[4]"
    var sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(s"SparkApp_${new Random().nextInt(10000)}")
      .set("spark.network.timeout", "6000s")
    val sc = new SparkContext(sparkConf)
    sc
  }
}

case class User(_id: String, key: String)

case class Post(_id: String, text: String, userId: String)

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
