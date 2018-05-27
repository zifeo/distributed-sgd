package epfl.distributed

import epfl.distributed.proto.Node
import epfl.distributed.core.{AsyncMaster, Master, SyncMaster}
import epfl.distributed.core.ml.{EarlyStopping, SparseSVM}
import epfl.distributed.math.Vec
import epfl.distributed.utils.Dataset.Data
import epfl.distributed.utils.{Config, Pool}

object Scenario {

  def launchMaster(node: Node, data: Data, model: SparseSVM, async: Boolean, config: Config): Unit = {
    val master = Master.create(node, data, model, async)
    master.start()

    sys.addShutdownHook {
      master.stop()
    }

    master.onSlaveJoin { slaveCount =>
      import Pool.AwaitableFuture
      val w0 = data(0)._1.zerosLike

      if (slaveCount == config.nodeCount) {
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
  }

}
