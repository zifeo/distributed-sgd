package epfl.distributed

import java.util.logging.LogManager

import com.typesafe.scalalogging.Logger
import epfl.distributed.core.ml.SparseSVM
import epfl.distributed.core.{Master, Slave}
import epfl.distributed.proto.Node
import epfl.distributed.utils.{Config, Dataset, Measure, Pool}
import kamon.Kamon
import kamon.influxdb.InfluxDBReporter

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

  // could use another model
  val model = new SparseSVM(config.lambda, config.learningRate / data.length)

  def scenario(master: Master): Unit = {

    val w0 = data(0)._1.zerosLike
    val l0 = master.computeLossDistributed(w0).await
    println(l0)
    val w1 = master.fit(w0, config).await
    println(w1)
    val l1 = master.computeLossDistributed(w1).await
    println(l1)

  }

  (config.masterHost, config.masterPort) match {

    case (Some(masterHost), Some(masterPort)) if masterHost == node.host && masterPort == node.port =>
      log.info("launch: only master")

      val master = Master(node, data, model, config.async, config.nodeCount)
      master.start()

      scenario(master)

      master.awaitTermination()

    case (Some(masterHost), Some(masterPort)) =>
      log.info("launch: only slave")

      val masterNode = Node(masterHost, masterPort)
      val slave      = new Slave(node, masterNode, data, model, config.async)
      slave.start()

      slave.awaitTermination()

    case _ =>
      log.info("launch: master + slaves (dev)")

      val masterNode :: slaveNodes =
        (0 until (1 + config.nodeCount)).map(i => Node(config.host, config.port + i)).toList
      val master = Master(masterNode, data, model, config.async, config.nodeCount)
      val slaves = slaveNodes.map(n => new Slave(n, masterNode, data, model, config.async))

      master.start()
      slaves.foreach(_.start().await)

      scenario(master)

      slaves.foreach(_.stop().await)
      master.stop()

  }

}
