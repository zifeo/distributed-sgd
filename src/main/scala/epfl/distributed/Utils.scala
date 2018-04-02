package epfl.distributed

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

object Utils {

  case class Config(port: Int)

  def newFixedExecutor(cores: Int = Runtime.getRuntime.availableProcessors()): ExecutionContext = {
    // number of threads should be of order cores^2
    val pool = Executors.newFixedThreadPool(if (cores > 1) cores * cores / 2 else 1)
    ExecutionContext.fromExecutorService(pool)
  }

}
