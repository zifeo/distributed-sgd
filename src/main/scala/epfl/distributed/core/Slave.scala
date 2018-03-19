package epfl.distributed.core

import epfl.distributed.config
import epfl.distributed.core.core._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Slave {

  val ip = "127.0.0.1"
  val port = 4000

  class SlaveImpl extends SlaveGrpc.Slave {

    // internal threadpool for work?

    def compute(request: ComputeRequest): Future[ComputeReply] = ???

  }

  // new thread pool for dispatcher
  val server = newServer(SlaveGrpc.bindService(new SlaveImpl, global), port)
  server.start()

  val channel = newChannel("127.0.0.1", config.port)
  val stub    = MasterGrpc.blockingStub(channel)

  val request = SlaveInfo(ip, port)
  println(stub.registerSlave(SlaveInfo(ip, port)))

}
