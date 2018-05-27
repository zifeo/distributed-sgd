package epfl.distributed.core

import com.google.protobuf.empty.Empty
import epfl.distributed.core.ml.EarlyStopping.EarlyStopping
import epfl.distributed.core.ml.{EarlyStopping, GradState, SparseSVM}
import epfl.distributed.math.Vec
import epfl.distributed.proto._
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
        log.info("waiting for slaves updates")
        masterGrpcImpl
          .startLossChecking(minStepsBetweenChecks = checkEvery)
          .runAsync(monix.execution.Scheduler.Implicits.global)
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
              val computedLoss = computeLossLocal(innerGradState.grad)
              val loss         = leakCoef * computedLoss + (1 - leakCoef) * losses.headOption.getOrElse(computedLoss)
              Kamon.counter("master.async.loss").increment(loss.toLong)

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
