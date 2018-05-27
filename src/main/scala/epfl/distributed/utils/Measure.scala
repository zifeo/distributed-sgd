package epfl.distributed.utils

import scala.concurrent.{ExecutionContext, Future}

object Measure {

  def currentSeconds: Double =
    System.currentTimeMillis() / 1e3

  def duration[T](f: () => T): (T, Double) = {
    val start = currentSeconds
    val res = f()
    val end = currentSeconds
    (res, end - start)
  }

  def duration[T](f: () => Future[T])(implicit ec: ExecutionContext): Future[(T, Double)] = {
    val start = currentSeconds
    f().map { res =>
      val end = currentSeconds
      (res, end - start)
    }
  }

}
