package epfl.distributed.core

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.Logger
import epfl.distributed.core.core._
import io.grpc.ManagedChannel

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Master(node: Node) {

  private val log    = Logger(s"master-${pretty(node)}")
  private val slaves = new ConcurrentHashMap[Node, ManagedChannel]()

  class MasterImpl extends MasterGrpc.Master {

    def registerSlave(node: Node): Future[Ack] = {
      log.info(s"new slave ${pretty(node)}")

      val channel = newChannel(node.ip, node.port)
      slaves.put(node, channel)
      Future.successful(Ack())
    }

    def unregisterSlave(node: Node): Future[Ack] = {
      log.info(s"exit slave ${pretty(node)}")

      slaves.remove(node)
      Future.successful(Ack())
    }

  }

  // new thread pool for dispatcher
  val server = newServer(MasterGrpc.bindService(new MasterImpl, global), node.port)
  server.start()

  log.info("ready")

  def compute(data: String): Future[String] = {

    val req = ComputeRequest("test")

    val pending = slaves.values().asScala.map { slaveChannel =>
      val stub = SlaveGrpc.stub(slaveChannel)
      stub.compute(req)
    }

    val results = Future.sequence(pending).map(_.map(_.result).mkString(", "))
    log.debug("compute reply")
    results
  }

}
