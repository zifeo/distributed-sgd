package epfl.distributed

import java.util.logging.LogManager

import com.typesafe.scalalogging.Logger
import epfl.distributed.Main.trainData
import epfl.distributed.core.ml.{EarlyStopping, SparseSVM, SplitStrategy}
import epfl.distributed.core.{Master, MasterAsync, MasterSync, Slave}
import epfl.distributed.math.{Sparse, SparseArrayVector, Vec}
import epfl.distributed.proto.Node
import epfl.distributed.utils.{Config, Dataset, Measure, Pool}
import kamon.Kamon
import kamon.influxdb.InfluxDBReporter
import spire.math.Number

import scala.util.Random

object Main extends App {

  import Pool.AwaitableFuture

  // init logback
  LogManager.getLogManager.readConfiguration()
  val log = Logger(s"bootstrap")

  {
    // host information
    val cores  = Runtime.getRuntime.availableProcessors()
    val memory = Runtime.getRuntime.maxMemory()
    log.info("cores: {}", cores)
    log.info("mem: {}G", if (memory != Long.MaxValue) memory / 1e9 else -1)
    Random.setSeed(0)
  }

  // load settings
  val config = pureconfig.loadConfigOrThrow[Config]("dsgd")
  val node   = Node(config.host, config.port)
  log.info("config: {}", config)

  if (config.record) {
    log.info("recording")
    Kamon.addReporter(new InfluxDBReporter())
  }

  log.info("loading data in: {}", config.dataPath)

  val (data, loadDuration) = Measure.duration {
    Dataset.rcv1(config.dataPath, full = config.full)
  }
  log.info("data loaded: {} ({}s)", data.length, loadDuration)

  val (trainData, testData) = data.splitAt((data.length * 0.8).toInt)

  val dimSparsity = Measure.durationLog(log, "dim sparsity") {
    val dim  = trainData(0)._1.size
    val buff = Array.fill(dim)(Number.zero)
    for {
      (v, _) <- trainData
      idx    <- v.map.keys
    } buff(idx - 1) += 1
    val inv = buff.zipWithIndex.collect {
      case (c, i) if c != Number.zero => i -> (1.0 / (c + 1))
    }.toMap
    Sparse(inv, dim)
  }

  // could use another model
  val model = new SparseSVM(config.lambda, dimSparsity)

  def scenario(master: Master): Unit = {

    val ss = SplitStrategy.vanilla

    val w0 = data(0)._1.zerosLike
    val l0 = master.distributedLoss(w0, ss).await
    log.info("initial loss: {}", l0)
    val a0 = master.distributedAccuracy(w0, ss).await
    log.info("initial accuracy: {}", a0)

    val w1 = Measure.durationLog(log, "fit") {
      val res = master match {
        case m: MasterAsync =>
          m.fit(
              initialWeights = w0,
              maxEpoch = config.maxEpochs,
              batchSize = config.batchSize,
              learningRate = config.learningRate,
              stoppingCriterion = EarlyStopping.noImprovement(
                  patience = config.patience,
                  minDelta = config.convDelta,
                  minSteps = None
              ),
              splitStrategy = ss,
              checkEvery = config.checkEvery,
              leakLossCoef = config.leakyLoss
          )
        case m: MasterSync =>
          m.fit(
              initialWeights = w0,
              maxEpochs = config.maxEpochs,
              batchSize = config.batchSize,
              learningRate = config.learningRate,
              stoppingCriterion = EarlyStopping.noImprovement(
                  patience = config.patience,
                  minDelta = config.convDelta,
                  minSteps = None
              ),
              splitStrategy = ss
          )
      }
      res.await.grad
    }

    log.info("final weights: {}", w1.map.map { case (idx, n) => s"$idx:$n" }.mkString(" "))
    val l1 = master.localLoss(w1, Some(testData))
    log.info("final test loss: {}", l1)
    val a1 = master.localAccuracy(w1, Some(testData))
    log.info("final test accuracy: {}", a1)

  }

  (config.masterHost, config.masterPort) match {

    case (Some(masterHost), Some(masterPort)) if masterHost == node.host && masterPort == node.port =>
      log.info("launch: only master")

      val master = Master(node, trainData, model, config.async, config.nodeCount)
      master.start()

      scenario(master)

      master.awaitTermination()

    case (Some(masterHost), Some(masterPort)) =>
      log.info("launch: only slave")

      val masterNode = Node(masterHost, masterPort)
      val slave      = new Slave(node, masterNode, trainData, model, config.async)
      slave.start()

      slave.awaitTermination()

    case _ =>
      log.info("launch: master + slaves (dev)")

      val masterNode :: slaveNodes =
        (0 until (1 + config.nodeCount)).map(i => Node(config.host, config.port + i)).toList
      val master = Master(masterNode, trainData, model, config.async, config.nodeCount)
      val slaves = slaveNodes.map(n => new Slave(n, masterNode, trainData, model, config.async))

      master.start()
      slaves.foreach(_.start().await)

      scenario(master)

      slaves.foreach(_.stop().await)
      master.stop()

  }

}
