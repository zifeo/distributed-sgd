package epfl.distributed.core

import com.typesafe.scalalogging.Logger
import epfl.distributed.Main.Data
import epfl.distributed.core.core._
import epfl.distributed.core.ml.SparseSVM
import epfl.distributed.data.Sparse

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Slave(node: Node, master: Node, data: Data, model: SparseSVM) {

  private val log = Logger(s"slave--${pretty(node)}")

  class SlaveImpl extends SlaveGrpc.Slave {

    // internal threadpool for work?

    def forward(request: ForwardRequest): Future[ForwardReply] = Future {
      val ForwardRequest(samplesIdx, weights) = request
      val w                                   = Sparse(weights, weights.size) //TODO What's the total vector size ?

      val preds = samplesIdx.map { idx =>
        val (x, y) = data(idx)
        model(w, x)
      }

      ForwardReply(preds.map(_.toDouble)) //TODO Possible loss of precision if Number was BigDecimal. Fix this
    }

    def gradient(request: GradientRequest): Future[GradientReply] = Future {
      val receivedAt                                         = System.currentTimeMillis()
      val GradientRequest(samplesIdx, step, lambda, weights) = request
      val w                                                  = Sparse(weights, weights.size) //TODO What's the total vector size ?

      val grad = samplesIdx
        .map { idx =>
          val (x, y) = data(idx)
          model.gradient(w, x, y)
        }
        .reduce(_ + _)

      val terminatedAt = System.currentTimeMillis()
      GradientReply((grad * -step).map.mapValues(_.toDouble), receivedAt, terminatedAt)
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
