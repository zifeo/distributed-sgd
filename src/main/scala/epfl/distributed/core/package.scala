package epfl.distributed

import epfl.distributed.proto.{Node, Sparse}
import epfl.distributed.math.Vec
import io.grpc._
import scalapb.TypeMapper
import spire.math.Number

package object core {

  implicit val numberTypeMapper: TypeMapper[Double, Number] = TypeMapper[Double, Number](Number.apply)(_.toDouble)
  implicit val vecTypeMapper: TypeMapper[Sparse, Vec] =
    TypeMapper[Sparse, Vec](sparse => Vec(sparse.map, sparse.size))(vec => Sparse(vec.map, vec.size))

  // create a new gRPC server and listen to given port
  def newServer(service: ServerServiceDefinition, port: Int): Server =
    ServerBuilder.forPort(port).addService(service).build

  // create a new channel to a gRPC server, disabling plaintext requires a certificate
  def newChannel(ip: String, port: Int): ManagedChannel =
    ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build

  def pretty(node: Node): String = {
    val Node(ip, port) = node
    val flatIp         = ip.replace(".", "")
    s"$flatIp:$port"
  }

}
