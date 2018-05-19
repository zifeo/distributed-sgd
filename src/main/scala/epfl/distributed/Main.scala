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

  val config = pureconfig.loadConfigOrThrow[Config]("dsgd")
  log.info("{}", config)
  log.info("cpu: {}", Runtime.getRuntime.availableProcessors())
  log.info("mem: {}G", Runtime.getRuntime.maxMemory() / 1e9)

  // current node (see application.conf, can be set using env vars)
  val node = Node(config.host, config.port)
  log.info("{}", node)

  val async = config.async // TODO : refactor into a strategy either sync or async
  log.info(if (config.async) "async" else "sync")

  // could use another model
  val model = new SparseSVM(0)

  if (config.record) {
    log.info("recording")
    Kamon.addReporter(new InfluxDBReporter())
  }

  val featuresCount = 47236

  val data: Array[(Vec, Int)] = Dataset.rcv1(config.dataPath, 100).map {
    case (x, y) => Vec(x, featuresCount) -> y
  }

  (config.masterHost, config.masterPort) match {

    case (Some(masterHost), Some(masterPort)) if masterHost == node.host && masterPort == node.port =>
      log.info("master")

      val master = Master.create(node, data, async)
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
              val w1 = asyncMaster.run(w0, 1e6.toInt, EarlyStopping.noImprovement(), 100, splitStrategy).await
              println(w1)

            case syncMaster: SyncMaster =>
              val epochs = 5

              println("Initial loss: " + syncMaster.computeLoss(w0).await)

              val w1 = syncMaster.backward(epochs = epochs, weights = w0).await

              println(s"End loss after $epochs epochs: " + syncMaster.computeLoss(w1).await)
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
      val master                   = Master.create(masterNode, data, async)
      val slaves                   = slaveNodes.map(n => new Slave(n, masterNode, data, model, async))

      master.start()
      slaves.foreach(_.start())

      val w0 = Vec.zeros(featuresCount)

      import Pool.AwaitableFuture

      master match {
        case asyncMaster: AsyncMaster =>
          val splitStrategy = (data: Array[(Vec, Int)], nSlaves: Int) =>
            data.indices.grouped(Math.round(data.length.toFloat / nSlaves)).toSeq
          val w1 = asyncMaster.run(w0, 1e6.toInt, EarlyStopping.noImprovement(), 100, splitStrategy).await
          println(w1)

        case syncMaster: SyncMaster =>
          val epochs = 5

          println("Initial loss: " + syncMaster.computeLoss(w0).await)

          val w1 = syncMaster.backward(epochs = epochs, weights = w0).await

          println(s"End loss after $epochs epochs: " + syncMaster.computeLoss(w1).await)
      }

      slaves.foreach(_.stop())
      master.stop()

  }

}
