package epfl.distributed.core


import com.typesafe.scalalogging.Logger
import epfl.distributed.core.core._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Slave(node: Node, master: Node) {

  private val log = Logger(s"slave--${pretty(node)}")


  class SlaveImpl extends SlaveGrpc.Slave {

    // internal threadpool for work?

    def compute(request: ComputeRequest): Future[ComputeReply] = {

      log.debug("compute request")

      val data = request.data
      val reply = ComputeReply(s"$data (${pretty(node)})")
      Future.successful(reply)
    }

  }

  // new thread pool for dispatcher
  val server = newServer(SlaveGrpc.bindService(new SlaveImpl, global), node.port)
  server.start()


  val masterChannel = newChannel(master.ip, master.port)
  val masterStub    = MasterGrpc.blockingStub(masterChannel)
  masterStub.registerSlave(node)

  log.info("ready and registered")



}
