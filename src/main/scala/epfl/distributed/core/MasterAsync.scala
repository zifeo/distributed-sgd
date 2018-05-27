package epfl.distributed.core

import com.google.protobuf.empty.Empty
import epfl.distributed.core.ml.EarlyStopping.EarlyStopping
import epfl.distributed.core.ml.{GradState, SparseSVM, SplitStrategy}
import epfl.distributed.math.Vec
import epfl.distributed.proto._
import epfl.distributed.utils.Dataset.Data
import kamon.Kamon
import monix.eval.Task
import spire.math.Number

import scala.concurrent.duration._
import scala.concurrent.stm._
import scala.concurrent.{Future, Promise}
import scala.util.Success

class MasterAsync(node: Node, data: Array[(Vec, Int)], model: SparseSVM, nodeCount: Int)
    extends Master(node, data, model, nodeCount: Int) {

  override protected val masterGrpcImpl = new AsyncMasterGrpcImpl

  /**
    * Starts the async computation of the weights
    *
    * @param initialWeights The initial weights
    * @param stoppingCriterion A function receiving (initial loss, current loss) and outputting whether to stop the computation
    * @param splitStrategy A function from (data, number workers) to a sequence of the assigned samples for each worker
    * @param checkEvery The number of gradient updates received by the master between loss checks
    *
    * @return The computed weights
    */
  def fit(initialWeights: Vec,
          maxEpoch: Int,
          batchSize: Int,
          learningRate: Double,
          stoppingCriterion: EarlyStopping,
          splitStrategy: SplitStrategy,
          checkEvery: Int,
          leakLossCoef: Double): Future[GradState] = {

    atomic { implicit txn =>
      require(!masterGrpcImpl.running, "Cannot start async computation: a computation is already running")

      log.info("starting async computation")
      val weightsPromise = Promise[GradState]
      masterGrpcImpl
        .initState(initialWeights, maxEpoch, stoppingCriterion, weightsPromise)

      val workers = slaves.values
      val split   = splitStrategy(data, workers.size)

      workers.zip(split).foreach {
        case (slave, assignment) =>
          slave.startAsync(StartAsyncRequest(initialWeights, assignment, batchSize, learningRate))
      }
      log.info("waiting for slaves updates")
      masterGrpcImpl
        .startLossChecking(minStepsBetweenChecks = checkEvery, leakLossCoef)
        .runAsync(monix.execution.Scheduler.Implicits.global)
      weightsPromise.future
    }
  }

  class AsyncMasterGrpcImpl extends AbstractMasterGrpc {

    private val gradState = Ref(GradState.empty)

    private val bestGrad = Ref(Vec.zeros(1))
    private val bestLoss = Ref(Number(Double.MaxValue))

    private var promise: Promise[GradState]      = _
    private var maxSteps: Int                    = _
    private var stoppingCriterion: EarlyStopping = _

    @inline def running: Boolean = gradState.single().end.isEmpty

    def initState(initialWeights: Vec,
                  initMaxSteps: Int,
                  initStoppingCriterion: EarlyStopping,
                  weightsPromise: Promise[GradState]): Unit = {
      gradState.single() = GradState.start(initialWeights)
      promise = weightsPromise
      maxSteps = initMaxSteps
      stoppingCriterion = initStoppingCriterion
    }

    def endComputation(): Unit = {
      atomic { implicit txn =>
        slaves.values.foreach(_.stopAsync(Empty()))

        promise.complete(Success(gradState.transformAndGet(_.replaceGrad(bestGrad()).finish(bestLoss()))))
        log.info("Async computation ended. Final loss: " + bestLoss())
      }
    }

    def startLossChecking(minStepsBetweenChecks: Long, leakCoef: Double): Task[Unit] = {
      require(0 <= leakCoef && leakCoef <= 1, "leaking coefficient must be between 0 and 1")

      def loop(lastStep: Long, losses: List[Number]): Task[Unit] =
        Task.defer {
          if (!running) {
            Task.unit
          }
          else {
            val innerGradState = gradState.single()

            if (innerGradState.updates - lastStep < minStepsBetweenChecks) { //Latest computation was too close
              log.warn(s"Latest step was too close. Last: $lastStep, current: ${innerGradState.updates}")
              loop(lastStep, losses).delayExecution(2.seconds)
            }
            else {
              val computedLoss = localLoss(innerGradState.grad)
              val loss         = leakCoef * computedLoss + (1 - leakCoef) * losses.headOption.getOrElse(computedLoss)
              Kamon.counter("master.async.loss").increment(loss.toLong)

              atomic { implicit txn =>
                // for early stopping: set best loss and related gradient
                val isBestLoss = bestLoss.transformIfDefined {
                  case oldLoss if oldLoss > loss => loss
                }
                if (isBestLoss) {
                  bestGrad.set(innerGradState.grad)
                }

                log.info(s"Steps: ${innerGradState.updates}, loss: $loss")
              }

              val newLosses = loss :: losses

              if (stoppingCriterion(newLosses)) { // converged => end computation
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

        if (newGradState.updates >= maxSteps) {
          log.info("max number of steps reached: stopping computation")
          endComputation()
        }
      }
      else {
        log.debug("Received gradient update after computation ended")
      }

      Future.successful(Ack())
    }
  }
}
