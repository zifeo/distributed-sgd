package epfl.distributed.core

import epfl.distributed.core.ml.SparseSVM
import epfl.distributed.math.Vec
import epfl.distributed.proto._

import scala.concurrent.Future

class MasterSync(node: Node, data: Array[(Vec, Int)], model: SparseSVM, nodeCount: Int)
    extends Master(node, data, model, nodeCount) {

  override protected val masterGrpcImpl = new SyncMasterGrpcImpl

  class SyncMasterGrpcImpl extends AbstractMasterGrpc {

    override def updateGrad(request: GradUpdate): Future[Ack] =
      throw new UnsupportedOperationException("Synchronous master cannot perform async operation update grad")
  }
}
