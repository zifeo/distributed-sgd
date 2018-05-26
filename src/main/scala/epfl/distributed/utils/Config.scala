package epfl.distributed.utils

case class Config(
    host: String,
    port: Int,
    masterHost: Option[String],
    masterPort: Option[Int],
    batchSize: Int,
    full: Boolean,
    async: Boolean,
    record: Boolean,
    dataPath: String,
)
