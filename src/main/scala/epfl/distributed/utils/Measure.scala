package epfl.distributed.utils

import scala.concurrent.{ExecutionContext, Future}
import com.typesafe.scalalogging.Logger

object Measure {

  def currentMs: Long =
    System.currentTimeMillis()

  def duration[T](f: => T): (T, Double) = {
    val start = currentMs
    val res = f
    val dur = currentMs - start
    (res, dur / 1e3)
  }

  def duration[T](f: => Future[T])(implicit ec: ExecutionContext): Future[(T, Double)] = {
    val start = currentMs
    f.map { res =>
      val dur = currentMs - start
      (res, dur / 1e3)
    }
  }

  def durationLog[T](log: Logger, name: String)(f: => T): T = {
    log.info("{} start", name)
    val start = currentMs
    val res = f
    val dur = (currentMs - start) / 1e3
    log.info("{} end ({}s)", name, dur)
    res
  }

}
