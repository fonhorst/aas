package itmo.escience.lect

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

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
    for ((uId, (sumlength_other, count_other)) <- other) {
      val (sumlength, count) = averPostLengthByUsers.getOrElse(uId, (0, 0))
      averPostLengthByUsers.update(uId, (sumlength + sumlength_other, count + count_other))
    }
  }

  override def value: mutable.HashMap[String, (Int, Int)] = averPostLengthByUsers.clone()
}