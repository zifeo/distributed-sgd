package epfl.distributed.data.dtypes

import spire.math.Number

import scala.language.implicitConversions

class NaiveSparseVector(val m: Map[Int, Double]) extends SparseVector[NaiveSparseVector] {

  require(m.values.forall(_ != 0.0), "sparse should not contain zero value" + m)

  val nonzero: Int             = m.size
  lazy val emptyLike: NaiveSparseVector = NaiveSparseVector.empty

  def +(b: Double): NaiveSparseVector =
    this.m.mapValues(_ + b).filter(_._2 != 0)

  def +(bs: NaiveSparseVector): NaiveSparseVector =
    (this.m.keySet ++ bs.m.keySet).flatMap { idx =>
      val ao = this.m.get(idx)
      val bo = bs.m.get(idx)
      val c = (ao, bo) match {
        case (Some(a), Some(b)) if a != b => Some(a + b)
        case (Some(a), None)    => Some(a)
        case (None, Some(a))    => Some(a)
        case _                  => None
      }
      c.map(idx -> _)
    }.toMap

  def *(b: Double): NaiveSparseVector =
    this.m.mapValues(_ * b).filter(_._2 != 0)

  def dot(bs: NaiveSparseVector): Double =
    (this.m.keySet ++ bs.m.keySet).flatMap { idx =>
      val ao = this.m.get(idx)
      val bo = bs.m.get(idx)
      (ao, bo) match {
        case (Some(a), Some(b)) => Some(a * b)
        case _                  => None
      }
    }.sum

  def sum: Double = m.values.sum

  override def toString: String = {
    val elems = m.take(10).map { case (k, v) => s"$k: $v" }.mkString(", ")
    val more = if (nonzero > 10) ", ..." else ""
    f"NaiveSparseVector($nonzero | $elems$more)"
  }

}

object NaiveSparseVector {

  val empty: NaiveSparseVector = new NaiveSparseVector(Map.empty[Int, Double])

  implicit def map2vec(m: Map[Int, Double]): NaiveSparseVector =
    new NaiveSparseVector(m)

}
