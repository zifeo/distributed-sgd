package epfl.distributed

import java.util.logging.LogManager

import com.typesafe.scalalogging.Logger
import epfl.distributed.core.core.Node
import epfl.distributed.core.ml.SparseSVM
import epfl.distributed.core.{Master, Slave}
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
  log.info("mem: {}", Runtime.getRuntime.maxMemory())

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

  val data: Array[(Vec, Int)] = Dataset.rcv1(config.dataPath, Some(100)).map {
    case (x, y) => Vec(x, featuresCount) -> y
  }

  (config.masterHost, config.masterPort) match {

    case (Some(masterHost), Some(masterPort)) if masterHost == node.host && masterPort == node.port =>
      log.info("master")

      val master = new Master(node, data, async)
      master.start()

      sys.addShutdownHook {
        master.stop()
      }

      master.onSlaveJoin { slaveCount =>
        if (slaveCount == 3) {
          import Pool.AwaitableFuture
          val epochs = 5

          val w0   = Vec.zeros(featuresCount)
          val res0 = master.forward(w0).await
          println("Initial loss: " + res0.zip(data).map { case (p, (_, y)) => Math.pow(p - y, 2) }.sum / data.length)

          val w1   = master.backward(epochs = epochs, weights = w0).await
          val res1 = master.forward(w1).await

          println(
              s"End loss after $epochs epochs: " + res1
                .zip(data)
                .map { case (p, (_, y)) => Math.pow(p - y, 2) }
                .sum / data.length)
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
      val master                   = new Master(masterNode, data, async)
      val slaves                   = slaveNodes.map(n => new Slave(n, masterNode, data, model, async))

      master.start()
      slaves.foreach(_.start())

      import Pool.AwaitableFuture
      val epochs = 5

      val w0   = Vec.zeros(featuresCount)
      val res0 = master.forward(w0).await
      println("Initial loss: " + res0.zip(data).map { case (p, (_, y)) => Math.pow(p - y, 2) }.sum / data.length)

      val w1   = master.backward(epochs = epochs, weights = w0).await
      val res1 = master.forward(w1).await

      println(
          s"End loss after $epochs epochs: " + res1
            .zip(data)
            .map { case (p, (_, y)) => Math.pow(p - y, 2) }
            .sum / data.length)

      slaves.foreach(_.stop())
      master.stop()

  }

}
