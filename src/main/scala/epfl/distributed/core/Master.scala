package epfl.distributed.core

import com.typesafe.scalalogging.Logger
import epfl.distributed.core.ml.EarlyStopping.EarlyStopping
import epfl.distributed.core.ml.{GradState, SparseSVM, SplitStrategy}
import epfl.distributed.math.Vec
import epfl.distributed.proto.SlaveGrpc.SlaveStub
import epfl.distributed.proto._
import epfl.distributed.utils.Dataset.Data
import epfl.distributed.utils.{Measure, Pool}
import io.grpc.Server
import kamon.Kamon
import spire.math.Number

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContextExecutorService, Future, Promise}
import scala.util.Random

abstract class Master(node: Node, data: Array[(Vec, Int)], model: SparseSVM, expectedNodeCount: Int) {

  protected val masterGrpcImpl: AbstractMasterGrpc

  protected val log                                          = Logger(s"mastr-${pretty(node)}")
  protected val slaves                                       = TrieMap[Node, SlaveStub]()
  implicit protected val ec: ExecutionContextExecutorService = Pool.newFixedExecutor()

  // without the lazy, we get an initialized field exception
  private lazy val server: Server = newServer(MasterGrpc.bindService(masterGrpcImpl, ec), node.port)

  private val clusterReadyPromise = Promise[Unit]()
  private val clusterReady        = clusterReadyPromise.future

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

  def withClusterReady[T](f: => Future[T]): Future[T] =
    clusterReady.flatMap(_ => f)

  def predict(weights: Vec, splitStrategy: SplitStrategy): Future[Map[Int, Number]] =
    withClusterReady {
      Measure.durationLogF(log, "forward") {
        val workers = slaves.values
        val split   = splitStrategy(data, workers.size)

        val work = workers.zip(split).map {
          case (worker, idx) =>
            val req = ForwardRequest(idx, weights)
            worker.forward(req).map(r => idx.zip(r.predictions))
        }

        Future.sequence(work).map(_.flatten.toMap)
      }
    }

  def distributedAccuracy(weights: Vec, splitStrategy: SplitStrategy): Future[Number] = {
    predict(weights, splitStrategy).map {
      _.map {
        case (i, p) =>
          val (_, y) = data(i)
          val label  = if (p > 0) 1 else -1

          if (label == y) 1.0 else 0.0
      }.sum / data.length
    }
  }

  def distributedLoss(weights: Vec, splitStrategy: SplitStrategy): Future[Number] = {
    predict(weights, splitStrategy)
      .map(_.map {
        case (i, p) =>
          val (_, y) = data(i)
          (p - y) ** 2
      }.reduce(_ + _) / data.length)

  }

  def localLoss(weights: Vec): Number = {
    data
      .map {
        case (x, y) =>
          (model(weights, x) - y) ** 2
      }
      .reduce(_ + _) / data.length
  }

  def localSampledLoss(weights: Vec, samplesCount: Int): Number = {
    (1 to samplesCount)
      .map { _ =>
        val (x, y) = data(Random.nextInt(data.length))
        (model(weights, x) - y) ** 2
      }
      .reduce(_ + _) / samplesCount
  }

  def fit(initialWeights: Vec,
          maxEpochs: Int,
          batchSize: Int,
          learningRate: Double,
          stoppingCriterion: EarlyStopping,
          splitStrategy: SplitStrategy): Future[GradState] =
    withClusterReady {
      Measure.durationLogF(log, "backward") {

        def loop[T](on: Seq[T])(init: Vec)(apply: (Vec, T) => Future[Vec]): Future[Vec] =
          on.foldLeft(Future.successful(init)) {
            case (itWeights, it) =>
              itWeights.flatMap(w => apply(w, it))
          }

        val workers = slaves.values
        val split   = splitStrategy(data, workers.size)

        val maxSamples = split.map(_.size).max

        def loopEpoch(epoch: Int,
                      epochWeight: GradState,
                      losses: List[Number],
                      accs: List[Number]): Future[GradState] = {

          if (losses.nonEmpty && accs.nonEmpty) {
            log.info("Loss after epoch {}: {}", epoch, losses.head)
            log.info("Acc after epoch {}: {}", epoch, accs.head)
          }

          if (epoch >= maxEpochs) {
            log.info("Reached max number of epochs: stopping computation")
            Future.successful(epochWeight.finish(losses.head))
          }
          else if (stoppingCriterion(losses)) {
            log.info("Converged to target: stopping computation")
            Future.successful(epochWeight.finish(losses.head))
          }
          else {
            val futureEpochWeight = loop(0 until maxSamples by batchSize)(epochWeight.grad) { // batch
              case (batchWeights, batch) =>
                log.info(s"samples ${batch + 1} - ${Math.min(batch + batchSize, maxSamples)} / $maxSamples")

                val timer = Kamon.timer("master.sync.batch.duration").start()
                val work = workers.zip(split).map {
                  case (worker, idx) =>
                    val req =
                      GradientRequest(batchWeights, idx.slice(batch, batch + batchSize))
                    worker.gradient(req)
                }
                Future
                  .sequence(work)
                  .map { res =>
                    timer.stop()
                    val grad     = Vec.mean(res.map(_.gradUpdate))
                    val sparsity = 100 * grad.sparsity()
                    log.info(f"sparsity $sparsity%.1f%%")
                    batchWeights - learningRate * grad
                  }
            }

            for {
              newEpochWeight <- futureEpochWeight
              loss           <- distributedLoss(newEpochWeight, splitStrategy)
              acc            <- distributedAccuracy(newEpochWeight, splitStrategy)
              newGrad        <- loopEpoch(epoch + 1, epochWeight.replaceGrad(newEpochWeight), loss :: losses, acc :: accs)
            } yield newGrad
          }
        }

        loopEpoch(0, GradState.start(initialWeights), List.empty, List.empty)

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
