package epfl.distributed.core

import com.typesafe.scalalogging.Logger
import epfl.distributed.Main.{Data, SparseVector}
import epfl.distributed.core.core._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Slave(node: Node, master: Node, data: Data) {

  private val log = Logger(s"slave--${pretty(node)}")

  class SlaveImpl extends SlaveGrpc.Slave {

    // internal threadpool for work?

    def compute(request: ComputeRequest): Future[ComputeReply] = {

      log.debug("compute request")

      val data  = request.data
      val reply = ComputeReply(s"$data (${pretty(node)})")
      Future.successful(reply)
    }

    def gradient(request: GradientRequest): Future[GradientReply] = Future {
      val receivedAt                                         = System.currentTimeMillis()
      val GradientRequest(samplesIdx, step, lambda, weights) = request

      val grad = samplesIdx
        .map { idx =>
          val (x, y) = data(idx)

          // need check
          val bal  = y * sparseDot(x, weights)
          val regu = sparseMult(weights, lambda * 2.0 / weights.size).values.sum
          val sub  = if (bal < 0) Map.empty: SparseVector else sparseMult(x, -step * y)
          sparseAddition(sub, regu)
        }
        .reduce(sparseAdd)

      val terminatedAt = System.currentTimeMillis()
      GradientReply(grad, receivedAt, terminatedAt)
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
