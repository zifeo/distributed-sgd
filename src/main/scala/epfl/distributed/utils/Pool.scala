package epfl.distributed.utils

import java.util.concurrent.Executors

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import kamon.executors.{Executors => KamonExecutors}

object Pool {

  def newFixedExecutor(cores: Int = Runtime.getRuntime.availableProcessors()): ExecutionContextExecutorService = {
    // number of threads should be of order cores^2
    val pool = Executors.newFixedThreadPool(8)//if (cores >= 4) 16 else if (cores > 1) cores * cores / 2 else 1)
    KamonExecutors.register("fixed-thread-pool", pool)
    ExecutionContext.fromExecutorService(pool)
  }

  implicit class AwaitableFuture[T](f: Future[T]) {
    def await: T = Await.result(f, Duration.Inf)
  }

}
