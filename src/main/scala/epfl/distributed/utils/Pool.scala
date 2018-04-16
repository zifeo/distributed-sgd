package epfl.distributed.utils

import java.util.concurrent.Executors

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object Pool {

  def newFixedExecutor(cores: Int = Runtime.getRuntime.availableProcessors()): ExecutionContext = {
    // number of threads should be of order cores^2
    val pool = Executors.newFixedThreadPool(if (cores > 1) cores * cores / 2 else 1)
    ExecutionContext.fromExecutorService(pool)
  }

  implicit class AwaitableFuture[T](f: Future[T]) {
    def await: T = Await.result(f, Duration.Inf)
  }

}
