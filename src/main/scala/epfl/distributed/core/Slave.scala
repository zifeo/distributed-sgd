package epfl.distributed.core

import com.typesafe.scalalogging.Logger
import epfl.distributed.core.ml.SparseSVM
import epfl.distributed.math.Vec
import epfl.distributed.proto.SlaveGrpc.SlaveStub
import epfl.distributed.proto._
import epfl.distributed.utils.Pool
import kamon.Kamon
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler}

import scala.collection.concurrent.TrieMap
import scala.concurrent.stm._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.Random

class Slave(node: Node, master: Node, data: Array[(Vec, Int)], model: SparseSVM, async: Boolean) {

  implicit val ec: ExecutionContextExecutorService = Pool.newFixedExecutor()
  private val log                                  = Logger(s"slave-${pretty(node)}")
  private val server                               = newServer(SlaveGrpc.bindService(new SlaveImpl, ec), node.port)
  private val masterStub                           = MasterGrpc.stub(newChannel(master.host, master.port))
  private val otherSlaves                          = TrieMap[Node, SlaveStub]()

  private val runningAsync = Ref(false)

  private var asyncComputation: CancelableFuture[Unit] = _
  private var assignedSamples: Seq[Int]                = _
  private var batchSize: Int                           = _

  private val weights = Ref(Vec.zeros(1))

  sys.addShutdownHook {
    this.stop()
  }

  def start(): Future[Unit] = {
    require(!ec.isShutdown)
    server.start()
    log.info("started")

    // register slave node
    masterStub.registerSlave(node).map { _ =>
      log.info("registered to master")
    }
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
      while (true) {
        val samples = if (batchSize == 1) {
          Seq(data(assignedSamples(Random.nextInt(assignedSamples.size))))
        }
        else {
          Random.shuffle[Int, IndexedSeq](assignedSamples.indices) take batchSize map data
        }

        val innerWeights = weights.single()
        val grads        = samples.map { case (x, y) => model.backward(innerWeights, x, y) }
        val gradUpdate   = model.learningRate * batchSize * Vec.sum(grads)

        weights.single.transform(_ - gradUpdate)

        val gradUpdateRequest = GradUpdate(gradUpdate)
        otherSlaves.values.foreach(_.updateGrad(gradUpdateRequest))
        Kamon.counter("slave.async.batch").increment()
        masterStub.updateGrad(gradUpdateRequest)

        log.trace("update sent")
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

      val preds = samplesIdx.map { idx =>
        val (x, y) = data(idx)
        model(w, x)
      }

      ForwardReply(preds)
    }

    def gradient(request: GradientRequest): Future[GradientReply] = Future {
      val receivedAt                     = System.currentTimeMillis()
      val GradientRequest(w, samplesIdx) = request

      val grad = samplesIdx
        .map { idx =>
          val (x, y) = data(idx)
          model.backward(w, x, y)
        }

      GradientReply(model.learningRate * Vec.sum(grad), receivedAt, System.currentTimeMillis())
    }

    def initAsync(request: AsyncInit): Future[Ack] = {
      require(async, "Cannot initialize async computation: slave is in synchronous mode.")

      atomic { implicit txn =>
        if (runningAsync()) {
          Future.failed(
              new IllegalStateException("Async computation already running, can't be initialized unless stopped first"))
        }
        else {
          runningAsync() = true

          log.info(
              s"initializing async computation. ${request.samples.size} samples assigned. Batch size: ${request.batchSize}")

          weights() = request.weights
          assignedSamples = request.samples
          batchSize = request.batchSize
          asyncComputation = asyncTask.runAsync(Scheduler(ec: ExecutionContext))

          Future.successful(Ack())
        }
      }
    }

    def updateGrad(request: GradUpdate): Future[Ack] = {
      require(async, "Cannot update gradient: slave is in synchronous mode.")

      log.trace("update received")

      weights.single.transform(_ - request.gradUpdate)
      Future.successful(Ack())
    }

    def stopAsync(request: com.google.protobuf.empty.Empty): Future[Ack] = {
      require(async, "Cannot stop async computation: slave is in synchronous mode.")
      log.debug("stopping async computation")

      asyncComputation.cancel()
      runningAsync.single() = false
      Future.successful(Ack())
    }

  }

}
