package epfl.distributed.utils

case class Config(
                   host: String,
                   port: Int,
                   masterHost: Option[String],
                   masterPort: Option[Int],
                   async: Boolean,
                   record: Boolean,
                   dataPath: String,
                 )

