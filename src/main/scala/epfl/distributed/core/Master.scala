package epfl.distributed.core

import com.google.protobuf.empty.Empty
import com.typesafe.scalalogging.Logger
import epfl.distributed.core.core.SlaveGrpc.SlaveStub
import epfl.distributed.core.core._
import epfl.distributed.core.ml.EarlyStopping.EarlyStopping
import epfl.distributed.core.ml.{EarlyStopping, GradState, SparseSVM}
import epfl.distributed.math.Vec
import epfl.distributed.utils.Pool
import io.grpc.Server
import monix.eval.Task
import spire.math.Number

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.stm._
import scala.concurrent.{ExecutionContextExecutorService, Future, Promise}
import scala.util.{Random, Success}

abstract class AbstractMaster(node: Node, data: Array[(Vec, Int)], model: SparseSVM) {

  protected val masterGrpcImpl: AbstractMasterGrpc

  implicit protected val ec: ExecutionContextExecutorService = Pool.newFixedExecutor()

  // without the lazy, we get an initialized field exception
  private lazy val server: Server = newServer(MasterGrpc.bindService(masterGrpcImpl, ec), node.port)
  private val slaveJoinCallbacks  = mutable.ListBuffer.empty[Int => Unit]

  protected val log    = Logger(s"master-${pretty(node)}")
  protected val slaves = TrieMap[Node, SlaveStub]()

  def start(): Unit = {
    require(!ec.isShutdown)
    server.start()
    log.info("started")
  }

  def stop(): Unit = {
    server.shutdown()
    server.awaitTermination()
    ec.shutdown()
    log.info("stopped")
  }

  def awaitTermination(): Unit = {
    log.info("waiting")
    server.awaitTermination()
  }

  def onSlaveJoin(callback: Int => Unit): Unit = {
    slaveJoinCallbacks += callback
  }

  def forward(weights: Vec): Future[Iterable[Number]] = {
    val workers = slaves.values
    val piece   = Math.floorDiv(data.length, workers.size)

    val work = workers.zipWithIndex.map {
      case (worker, i) =>
        val sample = i * piece
        val req    = ForwardRequest(sample until (sample + piece), weights)
        worker.forward(req)
    }

    Future.sequence(work).map(_.flatMap(_.predictions))
  }

  def backward(epochs: Int,
               batchSize: Int = 100,
               initialWeights: Vec,
               stoppingCriterion: EarlyStopping = EarlyStopping.noImprovement()): Future[GradState] = {

    def loop[T](on: Seq[T])(init: Vec)(apply: (Vec, T) => Future[Vec]): Future[Vec] =
      on.foldLeft(Future.successful(init)) {
        case (itWeights, it) =>
          itWeights.flatMap(w => apply(w, it))
      }

    log.info(s"dsgd start")

    val workersWithIndex = slaves.values.zipWithIndex
    val piece            = Math.floorDiv(data.length, workersWithIndex.size)

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
                val grad        = Vec.mean(res.map(_.grad))
                val durations   = res.map(x => x.terminatedAt - x.startedAt)
                val durationMax = durations.max / 1000.0
                val durationMin = durations.min / 1000.0
                val durationAvg = durations.sum / 1000.0 / durations.size
                val sparsity    = 100 * grad.sparsity()
                log.trace(
                    f"$epoch:$batch sparsity $sparsity%.1f%% duration ($durationMin%.3f, $durationAvg%.3f, $durationMax%.3f)")
                batchWeights - grad
              }
        }

        for {
          newEpochWeight <- futureEpochWeight
          loss           <- computeLossDistributed(newEpochWeight)
          newGrad        <- loopEpoch(epoch + 1, epochWeight.replaceGrad(newEpochWeight), loss :: losses)
        } yield newGrad
      }
    }

    val result = loopEpoch(1, GradState.start(initialWeights), Nil)
    //log.debug("Work done, waiting result")

    result.foreach(_ => log.info(s"dsgd end"))
    result
  }

  def computeLossDistributed(weights: Vec): Future[Number] = {
    forward(weights)
      .map(
          _.zip(data)
            .map { case (p, (_, y)) => (p - y) ** 2 }
            .reduce(_ + _) / data.length)
  }

  def computeLoss(weights: Vec, samplesCount: Option[Int] = None): Number = {
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

  def computeLoss(weights: Vec, samplesCount: Int): Number = computeLoss(weights, Some(samplesCount))

  abstract class AbstractMasterGrpc extends MasterGrpc.Master {

    def registerSlave(node: Node): Future[Ack] = {
      val stub = SlaveGrpc.stub(newChannel(node.host, node.port))

      val slavesSnap = slaves.readOnlySnapshot()
      slaves.put(node, stub)

      log.info(s"new slave ${pretty(node)}")

      slavesSnap.foreach {
        case (otherNode, otherStub) =>
          otherStub.registerSlave(node)
          stub.registerSlave(otherNode)
      }

      val ack = Future.successful(Ack())
      ack.onComplete(_ => slaveJoinCallbacks.foreach(_(slavesSnap.size + 1)))
      ack
    }

    def unregisterSlave(node: Node): Future[Ack] = {
      slaves.remove(node)

      log.info(s"exit slave ${pretty(node)}")

      slaves.values.foreach(_.unregisterSlave(node))

      Future.successful(Ack())
    }
  }
}

class SyncMaster(node: Node, data: Array[(Vec, Int)], model: SparseSVM) extends AbstractMaster(node, data, model) {

  override protected val masterGrpcImpl = new SyncMasterGrpcImpl

  class SyncMasterGrpcImpl extends AbstractMasterGrpc {

    override def updateGrad(request: GradUpdate): Future[Ack] =
      throw new UnsupportedOperationException("Synchronous master cannot perform async operation update grad")
  }
}

class AsyncMaster(node: Node, data: Array[(Vec, Int)], model: SparseSVM) extends AbstractMaster(node, data, model) {

  override protected val masterGrpcImpl = new AsyncMasterGrpcImpl

  protected case class AsyncConfig(initialWeights: Vec,
                                   maxSteps: Int,
                                   stoppingCriterion: EarlyStopping,
                                   batchSize: Int,
                                   splitStrategy: (Array[(Vec, Int)], Int) => Iterable[Seq[Int]],
                                   checkEvery: Int)

  /**
    * Starts the async computation of the weights
    *
    * @param initialWeights The initial weights
    * @param earlyStopping A function receiving (initial loss, current loss) and outputting whether to stop the computation
    * @param splitStrategy A function from (data, number workers) to a sequence of the assigned samples for each worker
    * @param checkEvery The number of gradient updates received by the master between loss checks
    *
    * @return The computed weights
    */
  def run(initialWeights: Vec,
          maxSteps: Int,
          earlyStopping: EarlyStopping = EarlyStopping.noImprovement(),
          batchSize: Int,
          splitStrategy: (Array[(Vec, Int)], Int) => Iterable[Seq[Int]],
          checkEvery: Int = 100): Future[GradState] = {
    atomic { implicit txn =>
      if (masterGrpcImpl.running) {
        Future.failed(new IllegalStateException("Cannot start async computation: a computation is already running"))
      }
      else {
        log.info("starting async computation")
        val weightsPromise = Promise[GradState]
        masterGrpcImpl.initState(
            initialWeights,
            AsyncConfig(initialWeights, maxSteps, earlyStopping, batchSize, splitStrategy, checkEvery),
            weightsPromise)

        val workers = slaves.values
        val split   = splitStrategy(data, workers.size)

        workers.zip(split).foreach {
          case (slave, assignment) => slave.initAsync(AsyncInit(initialWeights, assignment, batchSize))
        }
        masterGrpcImpl
          .startLossChecking(minStepsBetweenChecks = checkEvery)
          .runAsync(monix.execution.Scheduler.Implicits.global)
        log.info("waiting for slaves updates")
        weightsPromise.future
      }
    }
  }

  class AsyncMasterGrpcImpl extends AbstractMasterGrpc {

    private val gradState = Ref(GradState.empty)
    //private val losses    = Ref(ArrayBuffer.empty[Number])

    private val bestGrad = Ref(Vec.zeros(1))
    private val bestLoss = Ref(Number(Double.MaxValue))

    private var promise: Promise[GradState] = _
    private var asyncConfig: AsyncConfig    = _

    @inline def running: Boolean = gradState.single().end.isEmpty

    def initState(initialWeights: Vec, config: AsyncConfig, weightsPromise: Promise[GradState]): Unit = {
      gradState.single() = GradState.start(initialWeights)
      promise = weightsPromise
      asyncConfig = config
    }

    def endComputation(): Unit = {
      atomic { implicit txn =>
        slaves.values.foreach(_.stopAsync(Empty()))
        //losses.set(ArrayBuffer.empty[Number])

        promise.complete(Success(gradState.transformAndGet(_.replaceGrad(bestGrad()).finish(bestLoss()))))
        log.info("Async computation ended. Final loss: " + bestLoss())
      }
    }

    def startLossChecking(leakCoef: Double = 1, minStepsBetweenChecks: Long = 100): Task[Unit] = {
      def loop(lastStep: Long, losses: List[Number]): Task[Unit] =
        Task.defer {
          if (!running) {
            Task.unit
          }
          else {
            val innerGradState = gradState.single()

            if (innerGradState.updates - lastStep < minStepsBetweenChecks) { //Latest computation was too close
              log.trace(s"Latest step was too close. Last: $lastStep, current: ${innerGradState.updates}")
              loop(lastStep, losses).delayExecution(2.seconds)
            }
            else {
              val computedLoss = computeLoss(innerGradState.grad)
              val loss         = leakCoef * computedLoss + (1 - leakCoef) * losses.headOption.getOrElse(computedLoss)

              atomic { implicit txn =>
                //For early stopping: set best loss and related gradient
                val isBestLoss = bestLoss.transformIfDefined {
                  case oldLoss if oldLoss > loss => loss
                }
                if (isBestLoss) {
                  bestGrad.set(innerGradState.grad)
                }

                log.info(s"Steps: ${innerGradState.updates}, loss: $loss")
              }

              val newLosses = loss :: losses

              if (asyncConfig.stoppingCriterion(newLosses)) { //We converged => end computation
                log.info("converged to target: stopping computation")
                Task.now(endComputation())
              }
              else {
                loop(innerGradState.updates, newLosses)
              }
            }
          }
        }

      loop(-minStepsBetweenChecks, Nil)
    }

    def updateGrad(request: GradUpdate): Future[Ack] = {
      if (running) {
        val newGradState = gradState.single.transformAndGet(_.update(request.gradUpdate))

        log.trace(s"${newGradState.updates} updates received")

        if (newGradState.updates >= asyncConfig.maxSteps) {
          log.info("max number of steps reached: stopping computation")
          endComputation()
        }
        /*else if (newGradState.updates % asyncConfig.checkEvery == 0) {


          val loss = computeLoss(newGradState.grad, 5000)

          val newLosses = atomic { implicit txn =>
            //For early stopping: set best loss and related gradient
            val isBestLoss = bestLoss.transformIfDefined {
              case oldLoss if oldLoss > loss => loss
            }
            if (isBestLoss) {
              bestGrad.set(newGradState.grad)
            }

            /*computeFullLoss(newGradState.grad).onComplete {
              case Success(loss) =>*/
            log.info(s"Steps: ${newGradState.updates}, loss: $loss")
            losses.transformAndGet(_ += loss)
          }

          if (asyncConfig.stoppingCriterion(newLosses.toArray[Number])) { //We converged => end computation
            log.info("converged to target: stopping computation")
            endComputation()
          }
          /*
              case Failure(e) =>
                log.error("Failed to compute loss", e)
            }
         */
        }*/

        Future.successful(Ack())
      }
      else {
        log.debug("Received gradient update after computation ended")
        Future.successful(Ack())
        //Future.failed(new IllegalStateException("Master async computation stopped: won't accept any gradient updates"))
      }

    }
  }
}

object Master {

  def sync(node: Node, data: Array[(Vec, Int)], model: SparseSVM): SyncMaster = new SyncMaster(node, data, model)

  def async(node: Node, data: Array[(Vec, Int)], model: SparseSVM): AsyncMaster = new AsyncMaster(node, data, model)

  def create(node: Node, data: Array[(Vec, Int)], model: SparseSVM, async: Boolean): AbstractMaster = {
    if (async) {
      new AsyncMaster(node, data, model)
    }
    else {
      new SyncMaster(node, data, model)
    }
  }

}
