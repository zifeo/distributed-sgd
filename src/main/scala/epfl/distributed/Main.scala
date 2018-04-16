package epfl.distributed

import epfl.distributed.core.core.Node
import epfl.distributed.core.ml.SparseSVM
import epfl.distributed.core.{Master, Slave}
import epfl.distributed.math.{SparseArrayVector, Vec}
import epfl.distributed.utils.{Dataset, Pool}

object Main extends App {

  import Pool.AwaitableFuture

  type Data = Array[(Vec, Int)]
  val featuresCount = 47236

  val data: Data = Dataset.rcv1(500).map {
    case (x, y) => SparseArrayVector(x, featuresCount) -> y
  }

  val svm = new SparseSVM(0)

  // create nodes
  val masterNode :: slaveNodes = (0 to 3).map(p => Node("127.0.0.1", 4000 + p)).toList
  val master                   = new Master(masterNode, data)
  val slaves                   = slaveNodes.map(sn => new Slave(sn, masterNode, data, svm))

  val w0   = Vec.zeros(featuresCount)
  val res0 = master.forward(w0).await
  println(res0.zip(data).map { case (p, (_, y)) => Math.pow(p - y, 2) }.sum / data.length)

  var w = w0
  for (i <- 0 to 4) {
    val w1   = master.backward(epochs = 1, weights = w).await
    val res1 = master.forward(w1).await
    println(res1.zip(data).map { case (p, (_, y)) => Math.pow(p - y, 2) }.sum / data.length)
    w = w1
  }

}
