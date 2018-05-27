package epfl.distributed

import java.util.logging.LogManager

import com.typesafe.scalalogging.Logger
import epfl.distributed.core.core.Node
import epfl.distributed.core.ml.{EarlyStopping, SparseSVM}
import epfl.distributed.core.{AsyncMaster, Master, Slave, SyncMaster}
import epfl.distributed.math.Vec
import epfl.distributed.utils.{Config, Dataset, Pool}
import kamon.Kamon
import kamon.influxdb.InfluxDBReporter

object Main extends App {

  // init logback
  LogManager.getLogManager.readConfiguration()
  val log = Logger(s"bootstrap")

  // settings
  val config = pureconfig.loadConfigOrThrow[Config]("dsgd")
  log.info("config loaded: {}", config)

  {
    // host information
    val cores  = Runtime.getRuntime.availableProcessors()
    val memory = Runtime.getRuntime.maxMemory()
    log.info("cores: {}", cores)
    log.info("mem: {}G", if (memory != Long.MaxValue) memory / 1e9 else -1)
  }

  // current node (see application.conf, can be set using env vars)
  val node = Node(config.host, config.port)
  log.info("node: {}:{}", node.host, node.port)

  import config.async
  log.info("compute: {}", if (async) "async" else "sync")

  if (config.record) {
    log.info("recording")
    Kamon.addReporter(new InfluxDBReporter())
  }

  log.info("loading data in: {}", config.dataPath)
  val featuresCount = 47236

  val data: Array[(Vec, Int)] = Dataset.rcv1(config.dataPath, full = config.full).map {
    case (x, y) => Vec(x, featuresCount) -> y
  }
  log.info("data loaded: {}", data.length)

  // could use another model
  val model = new SparseSVM(config.lambda, config.learningRate / data.length)

  (config.masterHost, config.masterPort) match {

    case (Some(masterHost), Some(masterPort)) if masterHost == node.host && masterPort == node.port =>
      log.info("master")

      val master = Master.create(node, data, model, async)
      master.start()

      sys.addShutdownHook {
        master.stop()
      }

      master.onSlaveJoin { slaveCount =>
        import Pool.AwaitableFuture
        val w0 = Vec.zeros(featuresCount)

        if (slaveCount == 3) {
          master match {
            case asyncMaster: AsyncMaster =>
              val splitStrategy = (data: Array[(Vec, Int)], nSlaves: Int) =>
                data.indices.grouped(Math.round(data.length.toFloat / nSlaves)).toSeq
              val w1 =
                asyncMaster.run(w0, 1e6.toInt, EarlyStopping.noImprovement(), config.batchSize, splitStrategy).await
              println(w1)

            case syncMaster: SyncMaster =>
              println("Initial loss: " + syncMaster.computeLossDistributed(w0).await)

              val w1 = syncMaster.backward(epochs = 100, initialWeights = w0, batchSize = config.batchSize).await

              println(s"End loss after ${w1.updates} epochs: " + w1.loss.get)
          }
        }
      }

      master.awaitTermination()

    case (Some(masterHost), Some(masterPort)) =>
      log.info("slave")

      val masterNode = Node(masterHost, masterPort)
      val slave      = new Slave(node, masterNode, data, model, async)
      slave.start()

      sys.addShutdownHook {
        slave.stop()
      }

      slave.awaitTermination()

    case _ =>
      log.info("dev mode")

      val masterNode :: slaveNodes = (0 to 4).toList.map(i => Node(config.host, config.port + i))
      val master                   = Master.create(masterNode, data, model, async)
      val slaves                   = slaveNodes.map(n => new Slave(n, masterNode, data, model, async))

      master.start()
      slaves.foreach(_.start())
      Thread.sleep(2000) // ensure both slaves and master are launched in dev mode

      val w0 = Vec.zeros(featuresCount)

      import Pool.AwaitableFuture

      master match {
        case asyncMaster: AsyncMaster =>
          val splitStrategy = (data: Array[(Vec, Int)], nSlaves: Int) =>
            data.indices.grouped(Math.round(data.length.toFloat / nSlaves)).toSeq

          val w1 = asyncMaster.run(w0, 1e6.toInt, EarlyStopping.noImprovement(), config.batchSize, splitStrategy).await
          println(w1)
          println(asyncMaster.computeLoss(w1.grad, 5000))

        case syncMaster: SyncMaster =>
          println("Initial loss: " + syncMaster.computeLossDistributed(w0).await)

          val w1 = syncMaster.backward(epochs = 100, initialWeights = w0, batchSize = config.batchSize).await

          println(s"End loss after ${w1.updates} epochs: " + w1.loss.get)
      }

      slaves.foreach(_.stop())
      master.stop()

  }

}
