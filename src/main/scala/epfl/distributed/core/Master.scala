package epfl.distributed.core

import com.typesafe.scalalogging.Logger
import epfl.distributed.Main.Data
import epfl.distributed.core.core._
import epfl.distributed.core.ml.SparseSVM
import epfl.distributed.math.Vec
import epfl.distributed.utils.{Config, Pool}
import io.grpc.ManagedChannel

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class Master(data: Data) {

  val config = pureconfig.loadConfigOrThrow[Config]

  private val svm = new SparseSVM(0)

  private val node = Node("127.0.0.1", config.masterPort)

  private val log = Logger(s"master-${pretty(node)}")

  private val slaves = TrieMap[Node, ManagedChannel]()

  implicit val ec: ExecutionContext = Pool.newFixedExecutor()

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

  val server = newServer(MasterGrpc.bindService(new MasterImpl, ec), node.port)
  server.start()

  log.info("ready")

  config.slaves.addresses.zip(config.slaves.ports).foreach {
    case (address, port) => new Slave(Node(address, port), node, data, svm)
  }

  def forward(weights: Vec): Future[Array[Double]] = {
    val workers = slaves.values.map(SlaveGrpc.stub)
    val piece   = Math.floorDiv(data.length, workers.size)

    val work = workers.zipWithIndex.map {
      case (worker, i) =>
        val sample = i * piece
        //assert(!weights.map.mapValues(_.toDouble).exists(_._2.isNaN), "NaN detected in forward weights")
        val req = ForwardRequest(sample until (sample + piece), weights.map.mapValues(_.toDouble))
        worker.forward(req)
    }

    Future.sequence(work).map(_.flatMap(_.predictions).toArray)
  }

  def backward(epochs: Int, batch: Int = 1, weights: Vec): Future[Vec] = {
    log.info(s"dsgd start")
    //assert(!weights.map.mapValues(_.toDouble).exists(_._2.isNaN), "NaN detected in initial weights")

    val init             = Future.successful(weights)
    val workersWithIndex = slaves.values.map(SlaveGrpc.stub).zipWithIndex
    val piece            = Math.floorDiv(data.length, workersWithIndex.size)

    val result = (1 to epochs).foldLeft(init) {
      case (weightsEpoch, epoch) =>
        log.info(s"epoch $epoch")

        (0 until piece by batch).foldLeft(weightsEpoch) {
          case (weightStep, step) =>
            log.debug(s"step ${step + 1} / $piece")

            weightStep
              .flatMap { weights =>
                val work = workersWithIndex.map {
                  case (worker, i) =>
                    val sample = i * piece + step

                    //assert(!weights.map.mapValues(_.toDouble).exists(_._2.isNaN), "NaN detected in values")
                    val req =
                      GradientRequest(
                          sample until Math.min(sample + batch, i * piece + piece),
                          0.1,
                          0,
                          weights.map.mapValues(_.toDouble))
                    worker.gradient(req).map { res =>
                      require(!res.grad.values.exists(_.isNaN), "NaN detected")
                      res
                    }
                }
                Future
                  .sequence(work)
                  .map { res =>
                    val grad        = res.map(grad => Vec(grad.grad, weights.size)).fold(Vec.zeros(weights.size))(_ + _)
                    val durations   = res.map(x => x.terminatedAt - x.startedAt)
                    val durationMax = durations.max / 1000.0
                    val durationMin = durations.min / 1000.0
                    val durationAvg = durations.sum / 1000.0 / durations.size
                    val sparsity    = 100 * grad.sparsity()
                    log.debug(
                        f"$epoch:$step sparsity $sparsity%.1f%% duration ($durationMin%.3f, $durationAvg%.3f, $durationMax%.3f)")
                    weights + grad
                  }
              }
        }

    }
    log.debug("Work done, waiting result")

    result.foreach(_ => log.info(s"dsgd end"))
    result
  }

}
