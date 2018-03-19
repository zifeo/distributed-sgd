package epfl.distributed

import epfl.distributed.core.core.Node
import io.grpc._


package object core {

  def newServer(service: ServerServiceDefinition, port: Int): Server =
    ServerBuilder.forPort(port).addService(service).build

  def newChannel(ip: String, port: Int): ManagedChannel =
    ManagedChannelBuilder.forAddress(ip, port).usePlaintext(true).build

  def pretty(node: Node): String = {
    val Node(ip, port) = node
    val flatIp = ip.replace(".", "")
    s"$flatIp:$port"
  }

}
