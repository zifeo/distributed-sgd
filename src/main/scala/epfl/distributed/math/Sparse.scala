package epfl.distributed.math

import spire.math._

class Sparse private (override val map: Map[Int, Number], val size: Int) extends Vec {

  override def elementWiseOp(other: Vec, op: (Number, Number) => Number): Vec = {
    require(other.size == size, "Can't perform element-wise operation on vectors of different length")

    other match {
      case s: Sparse =>
        Sparse((map.keySet ++ s.map.keySet).map(idx => idx -> op(map(idx), s.map(idx))).toMap, size)

      case Dense(otherValues) =>
        map.foldLeft(Dense(otherValues)) {
          case (Dense(v), (i, n)) => Dense(v.updated(i, op(v(i), n)))
        }
    }
  }

  override def mapValues(op: Number => Number): Vec = {
    if (op(Number.zero) === Number.zero) {
      //Default value stays zero
      Sparse(map.mapValues(op), size)
    }
    else {
      //Default value changed => we need a dense vector because every index could have a value
      Dense((0 until size).map(idx => op(map(idx))))
    }
  }

  override def foldLeft[B](init: B)(op: (B, Number) => B): B = values.foldLeft(init)(op)

  override def sparse: Sparse = this

  def apply(idx: Int): Number = {
    if (idx < 0 || idx > size) {
      throw new IndexOutOfBoundsException(s"Illegal index '$idx'. Seriously ?")
    }
    else {
      map(idx)
    }
  }

  def apply(indices: Iterable[Int]): Sparse = Sparse(indices.map(i => i -> map(i)).toMap, size)

  override def nonZeroIndices(epsilon: Number = 1e-20): Iterable[Int] = {
    if (abs(epsilon) >= Sparse.epsilon) {
      map.keys
    }
    else {
      map.filter {
        case (_, num) => abs(num) > epsilon
      }.keys
    }
  }

  override def nonZeroCount(epsilon: Number = 1e-20): Int = {
    if (abs(epsilon) >= Sparse.epsilon) {
      map.size
    }
    else {
      map.count {
        case (_, num) => abs(num) > epsilon
      }
    }
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[Sparse]

  override def equals(that: Any): Boolean = that match {
    case sparse: Sparse => sparse.size == size && sparse.map == map
    case _              => false
  }
}

object Sparse {

  val epsilon: Number = 1e-20

  def apply(size: Int, values: (Int, Number)*): Sparse = Sparse(values.toMap, size)

  def apply(m: Map[Int, Number], size: Int): Sparse = {
    require(m.size <= size, "The sparse vector contains more elements than its defined size. Impossibru")

    new Sparse(
        m.filter {
            case (_, num) => abs(num) > Sparse.epsilon
          }
          .withDefaultValue(Number.zero),
        size
    )
  }

  def apply[A: Numeric](m: Map[Int, A], size: Int): Sparse = {
    val num = implicitly[Numeric[A]]
    Sparse(m.mapValues(num.toNumber), size)
  }

  def zeros(size: Int): Sparse                             = Sparse(Map[Int, Number](), size)
  def oneHot(value: Number, index: Int, size: Int): Sparse = Sparse(Map[Int, Number](index -> value), size)

}
