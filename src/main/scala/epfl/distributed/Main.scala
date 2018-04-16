package epfl.distributed

import epfl.distributed.core.Master
import epfl.distributed.math.Vec
import epfl.distributed.utils.{Dataset, Pool}

object Main extends App {
  import Pool.AwaitableFuture

  type Data = Array[(Vec, Int)]
  val featuresCount = 47236

  val data: Data = Dataset.rcv1(3000).map {
    case (x, y) => Vec(x, featuresCount) -> y
  }

  val master = new Master(data)

  val epochs = 5

  val w0   = Vec.zeros(featuresCount)
  val res0 = master.forward(w0).await
  println("Initial loss: " + res0.zip(data).map { case (p, (_, y)) => Math.pow(p - y, 2) }.sum / data.length)

  val w1   = master.backward(epochs = epochs, weights = w0).await
  val res1 = master.forward(w1).await

  println(
      s"End loss after $epochs epochs: " + res1
        .zip(data)
        .map { case (p, (_, y)) => Math.pow(p - y, 2) }
        .sum / data.length)

}
