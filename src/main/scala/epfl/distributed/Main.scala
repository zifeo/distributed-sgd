package epfl.distributed

import java.util.concurrent.TimeUnit

import epfl.distributed.core.core.Node
import epfl.distributed.core.{Master, Slave}
import epfl.distributed.data.Dataset

import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {

  println(Dataset.rcv1(1).mkString("\n"))

  val masterNode :: slaveNodes = (0 to 3).map(p => Node("127.0.0.1", 4000 + p)).toList

  val master = new Master(masterNode)
  val slaves = slaveNodes.map(sn => new Slave(sn, masterNode))

  master.compute("hello").foreach(println)

  master.server.awaitTermination(10, TimeUnit.SECONDS)

}
