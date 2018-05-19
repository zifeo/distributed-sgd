package epfl.distributed

import epfl.distributed.core.core.{Node, Sparse}
import epfl.distributed.math.Vec
import io.grpc._
import scalapb.TypeMapper
import spire.math.Number

package object core {

  implicit val numberTypeMapper = TypeMapper[Double, Number](Number(_))(_.toDouble)
  implicit val vecTypeMapper: TypeMapper[Sparse, Vec] =
    TypeMapper[Sparse, Vec](sparse => Vec(sparse.map, sparse.size))(vec => Sparse(vec.map, vec.size))

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
