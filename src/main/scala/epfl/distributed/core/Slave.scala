package epfl.distributed.core

import com.typesafe.scalalogging.Logger
import epfl.distributed.Main.Data
import epfl.distributed.core.core._
import epfl.distributed.core.ml.SparseSVM
import epfl.distributed.math.SparseArrayVector
import epfl.distributed.utils.Pool

import scala.concurrent.{ExecutionContext, Future}

class Slave(node: Node, master: Node, data: Data, model: SparseSVM) {

  val log                           = Logger(s"slave--${pretty(node)}")
  implicit val ec: ExecutionContext = Pool.newFixedExecutor()

  class SlaveImpl extends SlaveGrpc.Slave {

    def forward(request: ForwardRequest): Future[ForwardReply] = Future {
      val ForwardRequest(samplesIdx, weights) = request
      val w                                   = SparseArrayVector(weights, data.head._1.size)

      val preds = samplesIdx.map { idx =>
        val (x, y) = data(idx)
        model(w, x)
      }

      ForwardReply(preds.map(_.toDouble))
    }

    def gradient(request: GradientRequest): Future[GradientReply] = Future {
      val receivedAt                                         = System.currentTimeMillis()
      val GradientRequest(samplesIdx, step, lambda, weights) = request
      val w                                                  = SparseArrayVector(weights, data.head._1.size)

      val grad = samplesIdx
        .map { idx =>
          val (x, y) = data(idx)
          model.backward(w, x, y)
        }
        .reduce(_ + _)
      val gradResp = (grad * -step).map.mapValues(_.toDouble)
      require(!gradResp.values.exists(_.isNaN), "NaN detected in grad response")

      val terminatedAt = System.currentTimeMillis()
      GradientReply(gradResp, receivedAt, terminatedAt)
    }

  }

  val server = newServer(SlaveGrpc.bindService(new SlaveImpl, ec), node.port)
  server.start()

  // register slave node
  val masterChannel = newChannel(master.ip, master.port)
  val masterStub    = MasterGrpc.blockingStub(masterChannel)
  masterStub.registerSlave(node)

  log.info("ready and registered")

}
