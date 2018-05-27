package epfl.distributed.utils

import scala.concurrent.{ExecutionContext, Future}

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

}
