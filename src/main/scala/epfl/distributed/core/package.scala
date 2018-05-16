package epfl.distributed

import epfl.distributed.core.core.Node
import io.grpc._

package object core {

  // create a new gRPC server and listen to given port
  def newServer(service: ServerServiceDefinition, port: Int): Server =
    ServerBuilder.forPort(port).addService(service).build

  // create a new channel to a gRPC server, disabling plaintext requires a certificate
  def newChannel(ip: String, port: Int): ManagedChannel =
    ManagedChannelBuilder.forAddress(ip, port).usePlaintext().enableRetry().maxRetryAttempts(10).build

  def pretty(node: Node): String = {
    val Node(ip, port) = node
    val flatIp         = ip.replace(".", "")
    s"$flatIp:$port"
  }

}
