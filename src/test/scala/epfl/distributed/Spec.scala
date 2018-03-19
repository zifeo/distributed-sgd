package epfl.distributed

import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


trait Spec extends WordSpec
  with Matchers
  with Eventually {

  lazy implicit val timeout: FiniteDuration = 30.seconds

  implicit class AwaitableFuture[T](f: Future[T]) {
    def await: T = Await.result(f, timeout)
  }

}
