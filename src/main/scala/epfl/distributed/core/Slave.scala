package epfl.distributed.core

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import epfl.distributed.core.ml.SparseSVM
import epfl.distributed.math.Vec
import epfl.distributed.proto.SlaveGrpc.SlaveStub
import epfl.distributed.proto._
import epfl.distributed.utils.{Measure, Pool}
import kamon.Kamon
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler}

import scala.collection.concurrent.TrieMap
import scala.concurrent.stm._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Random, Success, Try}

class Slave(node: Node, master: Node, data: Array[(Vec, Int)], model: SparseSVM, async: Boolean) {

  private val log                                  = Logger(s"slave-${pretty(node)}")
  private val otherSlaves                          = TrieMap[Node, SlaveStub]()
  implicit val ec: ExecutionContextExecutorService = Pool.newFixedExecutor()

  private val server     = newServer(SlaveGrpc.bindService(new SlaveImpl, ec), node.port)
  private val masterStub = MasterGrpc.stub(newChannel(master.host, master.port))

  private val runningAsync                             = Ref(false)
  private val weights                                  = Ref(Vec.zeros(1))
  private var asyncComputation: CancelableFuture[Unit] = _
  private var assignedSamples: Seq[Int]                = _
  private var batchSize: Int                           = _
  private var learningRate: Double                     = _

  sys.addShutdownHook {
    this.stop()
  }

  def start(): Future[Unit] = {
    require(!ec.isShutdown)
    server.start()
    log.info("started")

    // register slave node
    def register: Future[Unit] =
      masterStub
        .withDeadlineAfter(5, TimeUnit.SECONDS)
        .registerSlave(node)
        .transformWith {
          case Success(_) =>
            log.info("registered to master")
            Future.successful(())
          case Failure(_) =>
            log.info("fail registration, retrying")
            Thread.sleep(2000)
            register
        }

    register
  }

  def awaitTermination(): Unit = {
    log.info("waiting")
    server.awaitTermination()
  }

  def stop(): Future[Unit] = {
    // unregister slave node
    masterStub.unregisterSlave(node).map { _ =>
      log.info("unregistered from master")

      server.shutdown()
      ec.shutdown()
      log.info("stopped")
    }
  }

  def asyncTask: Task[Unit] =
    Task {
      while (runningAsync.single()) {
        Measure.durationLog(log, "async backward") {
          val samples = if (batchSize == 1) {
            Seq(data(assignedSamples(Random.nextInt(assignedSamples.size))))
          }
          else {
            Random.shuffle[Int, IndexedSeq](assignedSamples.indices) take batchSize map data
          }

          val counter = Kamon.counter("slave.async.backward")

          val innerWeights = weights.single()
          val grads = samples.map {
            case (x, y) =>
              counter.increment()
              model.backward(innerWeights, x, y)
          }
          val grad       = Vec.mean(grads)
          val gradUpdate = learningRate * model.regularize(grad, innerWeights)

          weights.single.transform(_ - gradUpdate)

          val gradUpdateRequest = GradUpdate(gradUpdate)
          otherSlaves.values.foreach(_.updateGrad(gradUpdateRequest))
          masterStub.updateGrad(gradUpdateRequest)

          Kamon.counter("slave.async.batch").increment()
          log.trace("update sent")
        }
      }
    }

  class SlaveImpl extends SlaveGrpc.Slave {

    def registerSlave(node: Node): Future[Ack] = {
      otherSlaves.put(node, SlaveGrpc.stub(newChannel(node.host, node.port)))

      log.info(s"registered colleague slave ${pretty(node)}")
      Future.successful(Ack())
    }

    def unregisterSlave(node: Node): Future[Ack] = {
      otherSlaves.remove(node)

      log.info(s"unregistered colleague slave ${pretty(node)}")
      Future.successful(Ack())
    }

    def forward(request: ForwardRequest): Future[ForwardReply] = Future {
      val ForwardRequest(samplesIdx, w) = request
      val counter                       = Kamon.counter("slave.sync.forward")

      val preds = samplesIdx.map { idx =>
        val (x, y) = data(idx)
        counter.increment()
        model.forward(w, x)
      }

      ForwardReply(preds)
    }

    def gradient(request: GradientRequest): Future[GradUpdate] = Future {
      Measure.durationLog(log, "sync backward") {
        val GradientRequest(w, samplesIdx) = request
        val counter                        = Kamon.counter("slave.sync.backward")

        val grads = samplesIdx
          .map { idx =>
            val (x, y) = data(idx)
            counter.increment()
            model.backward(w, x, y)
          }
        val grad = Vec.sum(grads)

        GradUpdate(model.regularize(grad, w))
      }
    }

    def startAsync(request: StartAsyncRequest): Future[Ack] = {
      require(async, "Cannot initialize async computation: slave is in synchronous mode.")
      require(!runningAsync.single(), "Async computation already running, can't be initialized unless stopped first")

      atomic { implicit txn =>
        runningAsync() = true
        weights() = request.weights
      }

      assignedSamples = request.samples
      batchSize = request.batchSize
      learningRate = request.learningRate

      asyncComputation = asyncTask.runAsync(Scheduler(ec: ExecutionContext))
      log.info(s"init async computation, ${request.samples.size} samples assigned, batch size: ${request.batchSize}")
      Future.successful(Ack())
    }

    def updateGrad(request: GradUpdate): Future[Ack] = {
      require(async, "Cannot update gradient: slave is in synchronous mode.")

      weights.single.transform(_ - request.gradUpdate)
      Kamon.counter("slave.async.grad.update").increment()

      log.trace("update received")
      Future.successful(Ack())
    }

    def stopAsync(request: com.google.protobuf.empty.Empty): Future[Ack] = {
      require(async, "Cannot stop async computation: slave is in synchronous mode.")

      runningAsync.single() = false
      Try(asyncComputation.cancel())

      log.debug("stopping async computation")
      Future.successful(Ack())
    }

  }

}
