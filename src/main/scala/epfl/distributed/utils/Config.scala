package epfl.distributed.utils

import epfl.distributed.Main.config

case class Config(
    host: String,
    port: Int,
    masterHost: Option[String],
    masterPort: Option[Int],
    batchSize: Int,
    learningRate: Double,
    lambda: Double,
    nodeCount: Int,
    full: Boolean,
    async: Boolean,
    record: Boolean,
    dataPath: String,
    maxEpochs: Int,
    gossipInterval: Int,
    leakyLoss: Double,
    convDelta: Double,
    patience: Int
)
