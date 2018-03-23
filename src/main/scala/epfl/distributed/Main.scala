package epfl.distributed

import java.util.concurrent.TimeUnit

import epfl.distributed.core.core.Node
import epfl.distributed.core.{Master, Slave}
import epfl.distributed.data.Dataset

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Main extends App {

  type SparseVector = Map[Int, Double]
  type Target = Int
  type Data = Array[(SparseVector, Target)]

  val data = Dataset.rcv1(1000)

  val masterNode :: slaveNodes = (0 to 3).map(p => Node("127.0.0.1", 4000 + p)).toList

  val master = new Master(masterNode, data)
  val slaves = slaveNodes.map(sn => new Slave(sn, masterNode, data))

  master.gradient(1).onComplete {
    case Success(res) => println(res)
    case Failure(ex) => ex.printStackTrace()
  }

  master.server.awaitTermination(10, TimeUnit.SECONDS)

}
