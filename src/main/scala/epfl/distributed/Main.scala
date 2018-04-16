package epfl.distributed

import com.typesafe.scalalogging.Logger
import epfl.distributed.core.Master
import epfl.distributed.math.Vec
import epfl.distributed.utils.{Dataset, Pool}

object Main extends App {
  import Pool.AwaitableFuture

  private val log = Logger("Main")

  type Data = Array[(Vec, Int)]
  val featuresCount = 47236

  val data: Data = Dataset.rcv1(500).map {
    case (x, y) => Vec(x, featuresCount) -> y
  }

  val master = new Master(data)

  val w0   = Vec.zeros(featuresCount)
  val res0 = master.forward(w0).await
  println(res0.zip(data).map { case (p, (_, y)) => Math.pow(p - y, 2) }.sum / data.length)

  var w = w0
  for (i <- 0 to 4) {
    log.debug("Backward phase starting")
    val w1 = master.backward(epochs = 1, weights = w).await

    log.debug("Forward phase starting")
    val res1 = master.forward(w1).await

    println(res1.zip(data).map { case (p, (_, y)) => Math.pow(p - y, 2) }.sum / data.length)
    w = w1
  }

}
