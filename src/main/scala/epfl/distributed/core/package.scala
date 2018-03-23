package epfl.distributed

import epfl.distributed.Main.SparseVector
import epfl.distributed.core.core.Node
import io.grpc._

package object core {

  def newServer(service: ServerServiceDefinition, port: Int): Server =
    ServerBuilder.forPort(port).addService(service).build

  def newChannel(ip: String, port: Int): ManagedChannel =
    ManagedChannelBuilder.forAddress(ip, port).usePlaintext(true).build

  def pretty(node: Node): String = {
    val Node(ip, port) = node
    val flatIp         = ip.replace(".", "")
    s"$flatIp:$port"
  }

  // to replace, badly written

  def sparseAdd(as: SparseVector, bs: SparseVector): SparseVector =
    (as.keySet ++ bs.keySet).flatMap { idx =>
      val ao = as.get(idx)
      val bo = bs.get(idx)
      val c = (ao, bo) match {
        case (Some(a), Some(b)) => Some(a + b)
        case (Some(a), None)    => Some(a)
        case (None, Some(a))    => Some(a)
        case _                  => None
      }
      c.map(idx -> _)
    }.toMap

  def sparseAddition(as: SparseVector, b: Double): SparseVector =
    as.mapValues(_ + b)

  def sparseDot(as: SparseVector, bs: SparseVector): Double =
    (as.keySet ++ bs.keySet).flatMap { idx =>
      val ao = as.get(idx)
      val bo = bs.get(idx)

      (ao, bo) match {
        case (Some(a), Some(b)) => Some(a * b)
        case _                  => None
      }
    }.sum

  def sparseMult(as: SparseVector, b: Double): SparseVector =
    as.mapValues(_ * b)

}
