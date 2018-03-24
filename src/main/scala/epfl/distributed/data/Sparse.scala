package epfl.distributed.data

import spire.math._

case class Sparse(values: Map[Int, Number], size: Int) extends Vec {
  require(values.nonEmpty, "A vector cannot be empty")

  override def elementWiseOp(other: Vec, op: (Number, Number) => Number): Vec = {
    require(other.size == size, "Can't perform element-wise operation on vectors of differnet length")

    other match {
      case Sparse(otherValues, _) =>
        Sparse(
          values ++ otherValues.flatMap {
            case (i, v2) =>
              values.get(i).map(v1 => i -> op(v1, v2)).filter {
                case (_, v) => abs(v) > Sparse.epsilon
              }
          },
          size
        )

      case Dense(otherValues) =>
        values.foldLeft(Dense(otherValues)) {
          case (Dense(v), (i, n)) => Dense(v.updated(i, op(v(i), n)))
        }
    }
  }

  override def mapValues(op: Number => Number): Vec = Sparse(values.mapValues(op), size)

  override def foldLeft[B](init: B)(op: (B, Number) => B): B = values.values.foldLeft(init)(op)

  override def sparse: Sparse = this

  def apply(idx: Int): Number = {
    if (idx < 0 || idx > size) {
      throw new IndexOutOfBoundsException("Illegal index. Seriously ?")
    } else {
      values.getOrElse(idx, Number.zero)
    }
  }

  def apply(indices: Iterable[Int]): Sparse = Sparse(indices.map(i => i -> values(i)).toMap, size)

  def nonZeroIndices(epsilon: Number = 1e-20): Iterable[Int] = values.keys

}

object Sparse {

  val epsilon: Number = 1e-20

  def apply(size: Int, values: (Int, Number)*): Sparse = Sparse(values.toMap, size)

  def zeros(size: Int): Sparse               = Sparse(Map[Int, Number]().withDefaultValue(Number.zero), size)
  def ones(size: Int): Sparse                = Sparse(Map[Int, Number]().withDefaultValue(Number.one), size)
  def fill(value: Number, size: Int): Sparse = Sparse(Map[Int, Number]().withDefaultValue(value), size)
  def oneHot(value: Number, index: Int, size: Int): Sparse =
    Sparse(Map[Int, Number]().withDefaultValue(Number.zero) + (index -> value), size)

}
