package epfl.distributed.core

import java.util.concurrent.ConcurrentHashMap

import epfl.distributed.config
import epfl.distributed.core.core._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Master {

  val ip = "127.0.0.1"
  val port = 4000

  val slaves = ConcurrentHashMap.newKeySet[SlaveInfo]()

  class MasterImpl extends MasterGrpc.Master {

    def registerSlave(request: SlaveInfo): Future[Ack] = {
      slaves.add(request)
      Future.successful(Ack())
    }

    def unregisterSlave(request: SlaveInfo): Future[Ack] = {
      slaves.remove(request)
      Future.successful(Ack())
    }

  }

  // new thread pool for dispatcher
  val server = newServer(MasterGrpc.bindService(new MasterImpl, global), port)
  server.start()

  def compute(): Unit = {

    // for each slaves
    val channel = newChannel("127.0.0.1", config.port)
    val stub = MasterGrpc.blockingStub(channel)

    val request = SlaveInfo(ip, port)
    println(stub.registerSlave(SlaveInfo(ip, port)))

  }

}
