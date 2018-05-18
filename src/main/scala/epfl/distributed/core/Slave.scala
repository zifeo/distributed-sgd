package epfl.distributed.core

import com.typesafe.scalalogging.Logger
import epfl.distributed.core.core.SlaveGrpc.SlaveStub
import epfl.distributed.core.core._
import epfl.distributed.core.ml.SparseSVM
import epfl.distributed.math.Vec
import epfl.distributed.utils.Pool

import scala.collection.concurrent.TrieMap
import scala.concurrent.stm._
import scala.concurrent.{ExecutionContextExecutorService, Future}
import scala.util.Random

class Slave(node: Node, master: Node, data: Array[(Vec, Int)], model: SparseSVM, async: Boolean) {

  implicit val ec: ExecutionContextExecutorService = Pool.newFixedExecutor()
  private val log                                  = Logger(s"slave--${pretty(node)}")
  private val server                               = newServer(SlaveGrpc.bindService(new SlaveImpl, ec), node.port)
  private val masterChannel                        = newChannel(master.host, master.port)
  private val otherSlaves                          = TrieMap[Node, SlaveStub]()

  private val gamma = 1d / data.length

  private val runningAsync = Ref(false)

  private var assignedSamples: Seq[Int] = _
  private var batchSize: Int            = _

  private val weights = Ref(Vec.zeros(1))

  def start(): Unit = {
    require(!ec.isShutdown)
    server.start()
    log.info("started")

    // register slave node
    val masterStub = MasterGrpc.blockingStub(masterChannel)
    masterStub.registerSlave(node)
    log.info("registered")
  }

  def stop(): Unit = {
    // register slave node
    val masterStub = MasterGrpc.blockingStub(masterChannel)
    masterStub.unregisterSlave(node)
    log.info("unregistered")

    server.shutdown()
    server.awaitTermination()
    ec.shutdown()
    log.info("stopped")
  }

  def awaitTermination(): Unit = {
    log.info("waiting")
    server.awaitTermination()
  }

  def asyncComputation(): Unit = {
    val again = atomic { implicit txn =>
      if (runningAsync()) {
        val samples = if (batchSize == 1) {
          Seq(data(assignedSamples(Random.nextInt(assignedSamples.size))))
        }
        else {
          Random.shuffle[Int, IndexedSeq](assignedSamples.indices) take batchSize map data
        }

        val gradUpdate = Vec.sum(samples.map {
          case (x, y) => model.backward(weights(), x, y)
        }) * gamma

        weights.transform(_ - gradUpdate)
        otherSlaves.values.foreach(_.updateGrad(GradUpdate(gradUpdate)))

        true //Still running in async mode
      }
      else {
        false //The computation has stopped
      }
    }

    if (again) asyncComputation() else ()
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
      val receivedAt                                   = System.currentTimeMillis()
      val GradientRequest(samplesIdx, step, lambda, w) = request

      val grad = samplesIdx
        .map { idx =>
          val (x, y) = data(idx)
          model.backward(w, x, y)
        }
        .reduce(_ + _)

      GradientReply(grad * -step, receivedAt, System.currentTimeMillis())
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
          weights() = request.weights
          assignedSamples = request.samples
          batchSize = request.batchSize
          Future(asyncComputation())

          Future.successful(Ack())
        }
      }
    }

    def updateGrad(request: GradUpdate): Future[Ack] = {
      require(async, "Cannot update gradient: slave is in synchronous mode.")

      weights.single.transform(_ - request.gradUpdate)
      Future.successful(Ack())
    }

    def stopAsync(request: com.google.protobuf.empty.Empty): Future[Ack] = {
      require(async, "Cannot stop async computation: slave is in synchronous mode.")

      runningAsync.single() = false
      Future.successful(Ack())
    }

  }

}
