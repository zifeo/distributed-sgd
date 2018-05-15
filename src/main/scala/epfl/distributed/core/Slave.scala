package epfl.distributed.core

import com.typesafe.scalalogging.Logger
import epfl.distributed.core.core._
import epfl.distributed.core.ml.SparseSVM
import epfl.distributed.math.Vec
import epfl.distributed.utils.Pool

import scala.concurrent.{ExecutionContextExecutorService, Future}

class Slave(node: Node, master: Node, data: Array[(Vec, Int)], model: SparseSVM, async: Boolean) {

  implicit val ec: ExecutionContextExecutorService = Pool.newFixedExecutor()
  private val log                                  = Logger(s"slave--${pretty(node)}")
  private val server                               = newServer(SlaveGrpc.bindService(new SlaveImpl, ec), node.port)
  private val masterChannel                        = newChannel(master.host, master.port)

  def start(): Unit = {
    require(!ec.isShutdown)
    server.start()
    log.info("started")

    // register slave node
    val masterStub = MasterGrpc.blockingStub(masterChannel)
    masterStub.registerSlave(node)
    log.info("registered")
  }

  def stop(): Unit = {
    // register slave node
    val masterStub = MasterGrpc.blockingStub(masterChannel)
    masterStub.unregisterSlave(node)
    log.info("unregistered")

    server.shutdown()
    server.awaitTermination()
    ec.shutdown()
    log.info("stopped")
  }

  def awaitTermination(): Unit = {
    log.info("waiting")
    server.awaitTermination()
  }

  class SlaveImpl extends SlaveGrpc.Slave {

    def forward(request: ForwardRequest): Future[ForwardReply] = Future {
      val ForwardRequest(samplesIdx, weights) = request
      val w                                   = Vec(weights, data.head._1.size)

      val preds = samplesIdx.map { idx =>
        val (x, y) = data(idx)
        model(w, x)
      }

      ForwardReply(preds.map(_.toDouble))
    }

    def gradient(request: GradientRequest): Future[GradientReply] = Future {
      val receivedAt                                         = System.currentTimeMillis()
      val GradientRequest(samplesIdx, step, lambda, weights) = request
      val w                                                  = Vec(weights, data.head._1.size)

      val grad = samplesIdx
        .map { idx =>
          val (x, y) = data(idx)
          model.backward(w, x, y)
        }
        .reduce(_ + _)

      val gradResp = (grad * -step).map.mapValues(_.toDouble)

      GradientReply(gradResp, receivedAt, System.currentTimeMillis())
    }

  }

}
