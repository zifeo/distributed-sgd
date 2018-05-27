package epfl.distributed.core

import com.typesafe.scalalogging.Logger
import epfl.distributed.core.ml.EarlyStopping.EarlyStopping
import epfl.distributed.core.ml.{EarlyStopping, GradState, SparseSVM}
import epfl.distributed.math.Vec
import epfl.distributed.proto.SlaveGrpc.SlaveStub
import epfl.distributed.proto._
import epfl.distributed.utils.Dataset.Data
import epfl.distributed.utils.{Config, Measure, Pool}
import io.grpc.Server
import kamon.Kamon
import spire.math.Number

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContextExecutorService, Future, Promise}
import scala.util.Random

abstract class Master(node: Node, data: Array[(Vec, Int)], model: SparseSVM, expectedNodeCount: Int) {

  protected val masterGrpcImpl: AbstractMasterGrpc

  implicit protected val ec: ExecutionContextExecutorService = Pool.newFixedExecutor()

  // without the lazy, we get an initialized field exception
  private lazy val server: Server = newServer(MasterGrpc.bindService(masterGrpcImpl, ec), node.port)

  protected val log    = Logger(s"mastr-${pretty(node)}")
  protected val slaves = TrieMap[Node, SlaveStub]()

  private val clusterReadyPromise = Promise[Unit]()
  private val clusterReady                = clusterReadyPromise.future

  def withClusterReady[T](f: => Future[T]): Future[T] =
    clusterReady.flatMap(_ => f)

  sys.addShutdownHook {
    this.stop()
  }

  def start(): Unit = {
    require(!ec.isShutdown)
    server.start()
    log.info("started")
  }

  def awaitTermination(): Unit = {
    log.info("waiting")
    server.awaitTermination()
  }

  def stop(): Unit = {
    server.shutdown()
    ec.shutdown()
    log.info("stopped")
  }

  def forward(weights: Vec): Future[Iterable[Number]] =
    withClusterReady {
      Measure.durationLog(log, "forward") {
        val workers = slaves.values
        val piece = Math.floorDiv(data.length, workers.size)

        val work = workers.zipWithIndex.map {
          case (worker, i) =>
            val sample = i * piece
            val req = ForwardRequest(sample until (sample + piece), weights)
            worker.forward(req)
        }

        Future.sequence(work).map(_.flatMap(_.predictions))
      }
    }

  def backward(epochs: Int,
               batchSize: Int = 100,
               initialWeights: Vec,
               stoppingCriterion: EarlyStopping = EarlyStopping.noImprovement()): Future[GradState] =
    withClusterReady {
      Measure.durationLog(log, "backward") {

        def loop[T](on: Seq[T])(init: Vec)(apply: (Vec, T) => Future[Vec]): Future[Vec] =
          on.foldLeft(Future.successful(init)) {
            case (itWeights, it) =>
              itWeights.flatMap(w => apply(w, it))
          }

        val workersWithIndex = slaves.values.zipWithIndex
        val piece = Math.floorDiv(data.length, workersWithIndex.size)

        def loopEpoch(epoch: Int, epochWeight: GradState, losses: List[Number]): Future[GradState] = {
          losses.headOption.foreach(loss => log.info(s"Loss after epoch ${epoch - 1}: $loss"))

          if (epoch > epochs) {
            log.info("Reached max number of epochs: stopping computation")
            Future.successful(epochWeight.finish(losses.head))
          }
          else if (stoppingCriterion(losses)) {
            log.info("Converged to target: stopping computation")
            Future.successful(epochWeight.finish(losses.head))
          }
          else {
            val futureEpochWeight = loop(0 until piece by batchSize)(epochWeight.grad) { // batch
              case (batchWeights, batch) =>
                log.debug(s"samples ${batch + 1} - ${Math.min(batch + batchSize, piece)} / $piece")

                val timer = Kamon.timer("master.sync.batch.duration").start()
                val work = workersWithIndex.map {
                  case (worker, i) =>
                    val sample = i * piece + batch

                    val req =
                      GradientRequest(batchWeights, sample until Math.min(sample + batchSize, i * piece + piece))
                    worker.gradient(req)
                }
                Future
                  .sequence(work)
                  .map { res =>
                    timer.stop()
                    val grad = Vec.mean(res.map(_.grad))
                    val durations = res.map(x => x.terminatedAt - x.startedAt)
                    val durationMax = durations.max / 1000.0
                    val durationMin = durations.min / 1000.0
                    val durationAvg = durations.sum / 1000.0 / durations.size
                    val sparsity = 100 * grad.sparsity()
                    log.trace(
                      f"$epoch:$batch sparsity $sparsity%.1f%% duration ($durationMin%.3f, $durationAvg%.3f, $durationMax%.3f)")
                    batchWeights - grad
                  }
            }

            for {
              newEpochWeight <- futureEpochWeight
              loss <- computeLossDistributed(newEpochWeight)
              newGrad <- loopEpoch(epoch + 1, epochWeight.replaceGrad(newEpochWeight), loss :: losses)
            } yield newGrad
          }
        }

        val result = loopEpoch(1, GradState.start(initialWeights), Nil)
        //log.debug("Work done, waiting result")

        result
      }
    }

  def computeLossDistributed(weights: Vec): Future[Number] = {
    val loss = forward(weights)
      .map(
        _.zip(data)
          .map { case (p, (_, y)) => (p - y) ** 2 }
          .reduce(_ + _) / data.length)

    loss
  }

  def computeLossLocal(weights: Vec, samplesCount: Option[Int] = None): Number = {
    samplesCount match {
      case Some(count) =>
        (1 to count)
          .map { _ =>
            val (x, y) = data(Random.nextInt(data.length))
            (model(weights, x) - y) ** 2
          }
          .reduce(_ + _) / count

      case None =>
        data
          .map {
            case (x, y) =>
              (model(weights, x) - y) ** 2
          }
          .reduce(_ + _) / data.length
    }
  }

  def computeLossLocal(weights: Vec, samplesCount: Int): Number = computeLossLocal(weights, Some(samplesCount))

  def fit(w0: Vec, config: Config): Future[Vec] =
    withClusterReady {
      Measure.durationLog(log, "fit") {
        val w1 = this match {
          case asyncMaster: MasterAsync =>
            val splitStrategy =
              (data: Data, nSlaves: Int) => data.indices.grouped(Math.round(data.length.toFloat / nSlaves)).toSeq

            asyncMaster.run(w0, 1e6.toInt, EarlyStopping.noImprovement(), config.batchSize, splitStrategy)

          case syncMaster: MasterSync =>
            syncMaster.backward(epochs = 100, initialWeights = w0, batchSize = config.batchSize)
        }
        w1.map(_.grad)
      }
    }

  abstract class AbstractMasterGrpc extends MasterGrpc.Master {

    def registerSlave(node: Node): Future[Ack] = {
      val slavesSnap = slaves.readOnlySnapshot()
      require(slaves.size <= expectedNodeCount, "too many nodes have joined")

      val stub = SlaveGrpc.stub(newChannel(node.host, node.port))
      slaves.put(node, stub)

      slavesSnap.foreach {
        case (otherNode, otherStub) =>
          otherStub.registerSlave(node)
          stub.registerSlave(otherNode)
      }

      if (slaves.size >= expectedNodeCount & !clusterReady.isCompleted) {
        log.info("cluster is now ready ({}/{} slaves, new {})", slaves.size, expectedNodeCount, pretty(node))
        clusterReadyPromise.trySuccess(())
      }
      else {
        log.info("cluster is waiting ({}/{} slaves, new {})", slaves.size, expectedNodeCount, pretty(node))
      }
      Future.successful(Ack())
    }

    def unregisterSlave(node: Node): Future[Ack] = {
      slaves.remove(node)

      log.info(s"exit slave ${pretty(node)}")

      slaves.values.foreach(_.unregisterSlave(node))

      Future.successful(Ack())
    }
  }
}

object Master {

  def apply(node: Node, data: Array[(Vec, Int)], model: SparseSVM, async: Boolean, nodeCount: Int): Master = {
    if (async) {
      new MasterAsync(node, data, model, nodeCount)
    }
    else {
      new MasterSync(node, data, model, nodeCount)
    }
  }

}
