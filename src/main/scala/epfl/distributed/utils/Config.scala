package epfl.distributed.utils

case class Config(dataPath: String, masterPort: Int, slaves: SlavesConfig)

case class SlavesConfig(addresses: Seq[String], ports: Seq[Int]) {
  require(addresses.size == ports.size, "Should have the same number of slave addresses as ports")
}