package epfl.distributed.core

import com.typesafe.scalalogging.Logger
import epfl.distributed.core.core.SlaveGrpc.SlaveStub
import epfl.distributed.core.core._
import epfl.distributed.math.Vec
import epfl.distributed.utils.Pool

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.stm._
import scala.concurrent.{ExecutionContextExecutorService, Future, Promise}

class Master(node: Node, data: Array[(Vec, Int)], async: Boolean) {

  implicit val ec: ExecutionContextExecutorService = Pool.newFixedExecutor()
  private val log                                  = Logger(s"master-${pretty(node)}")
  private val slaves                               = TrieMap[Node, SlaveStub]()
  private val server                               = newServer(MasterGrpc.bindService(new MasterImpl, ec), node.port)

  private val runningAsync = Ref(false)
  private val asyncUpdates = Ref(0)
  private val grad = Ref(Vec.zeros(1))
  private var asyncWeightsPromise: Promise[Vec] = _

  // need to change this
  private val slaveJoinCallbacks = mutable.ListBuffer.empty[Int => Unit]

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

  /**
    * Starts the async computation of the weights
    *
    * @param initialWeights The initial weights
    * @param stoppingCriterion A function receiving (initial loss, current loss) and outputting whether to stop the computation
    * @param checkEvery The number of gradient updates received by the master between loss checks
    *
    * @return The computed weights
    */
  def async(initialWeights: Vec, stoppingCriterion: (Number, Number) => Boolean, checkEvery: Int = 100): Future[Vec] = {
    atomic { implicit txn =>
      if(runningAsync()){
        Future.failed(new IllegalStateException("Cannot start async computation: a computation is already running"))
      }
      else {
        runningAsync() = true
        grad() = initialWeights
        asyncWeightsPromise = Promise[Vec]

        slaves.values.foreach(_.initAsync(AsyncInit(initialWeights, ???))) //TODO samples assignment

        asyncWeightsPromise.future
      }
    }
  }

  def checkLoss: Number = ???

  def forward(weights: Vec): Future[Array[Double]] = {
    val workers = slaves.values
    val piece   = Math.floorDiv(data.length, workers.size)

    val work = workers.zipWithIndex.map {
      case (worker, i) =>
        val sample = i * piece
        val req    = ForwardRequest(sample until (sample + piece), weights)
        worker.forward(req)
    }

    Future.sequence(work).map(_.flatMap(_.predictions).toArray)
  }

  def backward(epochs: Int, batchSize: Int = 1, weights: Vec): Future[Vec] = {

    def loop[T](on: Seq[T])(init: Vec)(apply: (Vec, T) => Future[Vec]): Future[Vec] =
      on.foldLeft(Future.successful(init)) {
        case (itWeights, it) =>
          itWeights.flatMap(w => apply(w, it))
      }

    /*
      Need to improve:
      - data idx attributions per node
      - randomness of idx between epochs
      - cleaner encoding/decoding between vec implement and grpc message data
      - stepsize

      How can this fit into the async mode?
     */

    log.info(s"dsgd start")

    val init             = weights
    val workersWithIndex = slaves.values.zipWithIndex
    val piece            = Math.floorDiv(data.length, workersWithIndex.size)

    val result = loop(1 to epochs)(init) { // epoch
      case (epochWeight, epoch) =>
        log.info(s"epoch $epoch")

        loop(0 until piece by batchSize)(epochWeight) { // batch
          case (batchWeights, batch) =>
            log.debug(s"step ${batch + 1} / $piece")

            val work = workersWithIndex.map {
              case (worker, i) =>
                val sample = i * piece + batch

                val req =
                  GradientRequest(sample until Math.min(sample + batchSize, i * piece + piece), 0.1, 0, batchWeights)
                worker.gradient(req)
            }
            Future
              .sequence(work)
              .map { res =>
                val grad        = Vec.mean(res.map(_.grad.get))
                val durations   = res.map(x => x.terminatedAt - x.startedAt)
                val durationMax = durations.max / 1000.0
                val durationMin = durations.min / 1000.0
                val durationAvg = durations.sum / 1000.0 / durations.size
                val sparsity    = 100 * grad.sparsity()
                log.trace(
                    f"$epoch:$batch sparsity $sparsity%.1f%% duration ($durationMin%.3f, $durationAvg%.3f, $durationMax%.3f)")
                batchWeights + grad
              }
        }

    }
    log.debug("Work done, waiting result")

    result.foreach(_ => log.info(s"dsgd end"))
    result
  }

  class MasterImpl extends MasterGrpc.Master {

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

    def updateGrad(request: GradUpdate): scala.concurrent.Future[Ack] = {
      require(async, "Cannot update gradient: Master is in synchronous mode.")

      atomic {implicit txn =>
        grad.transform(_ - request.gradUpdate)
        asyncUpdates += 1

        //TODO Implement stopping criterion
        if(asyncUpdates() % ??? == 0){
          checkLoss
        }
      }

      Future.successful(Ack())
    }
  }
}
