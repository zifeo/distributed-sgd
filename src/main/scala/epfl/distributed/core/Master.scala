package epfl.distributed.core

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.Logger
import epfl.distributed.Main.{Data, SparseVector}
import epfl.distributed.core.core._
import io.grpc.ManagedChannel

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Master(node: Node, data: Data) {

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

  def gradient(epochs: Int, batch: Int = 1): Future[SparseVector] = {
    log.info(s"dsgd start")

    val init    = Future.successful(Map.empty: SparseVector)
    val workers = slaves.values().asScala.map(SlaveGrpc.stub)
    val piece   = Math.floorDiv(data.length, workers.size)
    val dims    = data.flatMap(_._1.keys).max

    log.info(s"dims $dims")

    val result = (0 until epochs).foldLeft(init) {
      case (weightsEpoch, epoch) =>
        log.info(s"epoch $epoch")

        (0 until piece by batch).foldLeft(weightsEpoch) {
          case (weightStep, step) =>
            //log.debug(s"step $step")

            weightStep
              .flatMap { weights =>
                val work = workers.zipWithIndex.map {
                  case (worker, i) =>
                    val sample = i * piece + step
                    val req    = GradientRequest(sample until Math.min(sample + batch, i * piece + piece), 0.001, 0, weights)
                    worker.gradient(req).map { res =>
                      require(!res.grad.values.exists(_.isNaN), "NaN detected")
                      res
                    }
                }
                Future.sequence(work)
              }
              .map { res =>
                val grad        = res.map(_.grad: SparseVector).reduce(sparseAdd)
                val durations   = res.map(x => x.terminatedAt - x.startedAt)
                val durationMax = durations.max / 1000.0
                val durationMin = durations.min / 1000.0
                val durationAvg = durations.sum / 1000.0 / durations.size
                val sparsity    = if (grad.isEmpty) 100 else 100 - 100 * grad.size.toDouble / dims
                log.debug(f"$epoch.$step duration $sparsity%.2f ($durationMin%.3f, $durationAvg%.3f, $durationMax%.3f)")
                grad
              }
        }

    }

    result.map { res =>
      log.info(s"dsgd end")
      res
    }
  }

}
