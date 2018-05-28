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

abstract class Master(node: Node,
                      data: Array[(Vec, Int)],
                      testData: Array[(Vec, Int)],
                      model: SparseSVM,
                      expectedNodeCount: Int) {

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

  def distributedAccuracy(weights: Vec, splitStrategy: SplitStrategy): Future[Double] = {
    predict(weights, splitStrategy).map { preds =>
      preds.count {
        case (i, p) =>
          val (_, y) = data(i)
          p === y
      }.toDouble / preds.size
    }
  }

  def distributedLoss(weights: Vec, splitStrategy: SplitStrategy): Future[Number] = {
    predict(weights, splitStrategy)
      .map { preds =>
        model.lambda * weights.normSquared + preds
          .map {
            case (i, p) =>
              val (_, y) = data(i)
              model.loss(p, y)
          }
          .reduce(_ + _) / preds.size
      }
  }

  def localAccuracy(weights: Vec, testData: Option[Data] = None): Double = {
    val workingData = testData.getOrElse(data)
    workingData.count { case (x, y) => model.forward(weights, x) === y }.toDouble / workingData.length
  }

  def localLoss(weights: Vec, testData: Option[Data] = None): Number = {
    model.loss(weights, testData.getOrElse(data))
  }

  def localSampledLoss(weights: Vec, samplesCount: Int, testData: Option[Data] = None): Number = {
    val workingData = testData.getOrElse(data)
    model.loss(weights, Random.shuffle[Int, IndexedSeq](workingData.indices) take samplesCount map workingData)
  }

  def localSampledAccuracy(weights: Vec, samplesCount: Int, testData: Option[Data] = None): Number = {
    val workingData = testData.getOrElse(data)
    val samples     = Random.shuffle[Int, IndexedSeq](workingData.indices) take samplesCount map workingData
    samples.count { case (x, y) => model.forward(weights, x) === y }.toDouble / samples.length
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
                      accs: List[Double],
                      testLosses: List[Number],
                      testAccs: List[Double]): Future[GradState] = {

          if (losses.nonEmpty && accs.nonEmpty) {
            log.info("loss after epoch {}: {}", epoch, losses.head)
            log.info("acc after epoch {}: {}", epoch, accs.head)
            Kamon.histogram("master.sync.loss").record(losses.head.toLong)
            Kamon.histogram("master.sync.acc").record(100 * accs.head.toLong)
          }

          if (epoch >= maxEpochs) {
            log.info("Reached max number of epochs: stopping computation")
            log.info("Losses: {}", losses.reverse.mkString(", "))
            log.info("Test losses: {}", testLosses.reverse.mkString(", "))
            log.info("Accuracies: {}", accs.reverse.mkString(", "))
            log.info("Test Accuracies: {}", testAccs.reverse.mkString(", "))
            println(losses.reverse.mkString(", "))
            println(testLosses.reverse.mkString(", "))
            println(accs.reverse.mkString(", "))
            println(testAccs.reverse.mkString(", "))
            Future.successful(epochWeight.finish(losses.head))
          }
          else if (stoppingCriterion(testLosses)) {
            log.info("Converged to target: stopping computation")
            log.info("Losses: {}", losses.reverse.mkString(", "))
            log.info("Test losses: {}", testLosses.reverse.mkString(", "))
            log.info("Accuracies: {}", accs.reverse.mkString(", "))
            log.info("Test Accuracies: {}", testAccs.reverse.mkString(", "))
            println(losses.reverse.mkString(", "))
            println(testLosses.reverse.mkString(", "))
            println(accs.reverse.mkString(", "))
            println(testAccs.reverse.mkString(", "))
            Future.successful(epochWeight.finish(losses.head))
          }
          else {
            val futureEpochWeight = loop(0 until maxSamples by batchSize)(epochWeight.grad) { // batch
              case (batchWeights, batch) =>
                log.info(s"samples ${batch + 1} - ${Math.min(batch + batchSize, maxSamples)} / $maxSamples")

                val timer = Kamon.timer("master.sync.batch.duration").start()
                val work = workers.zip(split.map(Random.shuffle(_))).map {
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
              newGrad <- loopEpoch(
                            epoch + 1,
                            epochWeight.replaceGrad(newEpochWeight),
                            localLoss(newEpochWeight) :: losses,
                            localAccuracy(newEpochWeight) :: accs,
                            localLoss(newEpochWeight, Some(testData)) :: testLosses,
                            localAccuracy(newEpochWeight, Some(testData)) :: testAccs
                        )
            } yield newGrad
          }
        }

        loopEpoch(0, GradState.start(initialWeights), List.empty, List.empty, Nil, Nil)

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

  def apply(node: Node,
            data: Array[(Vec, Int)],
            testData: Array[(Vec, Int)],
            model: SparseSVM,
            async: Boolean,
            nodeCount: Int): Master = {
    if (async) {
      new MasterAsync(node, data, testData, model, nodeCount)
    }
    else {
      new MasterSync(node, data, testData, model, nodeCount)
    }
  }

}
