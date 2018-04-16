package epfl.distributed.math

import spire.math._

case class SparseArrayVector(sparseVec: (List[Int], List[Number]), size: Int) extends Vec {

  def elementWiseOp(other: Vec, op: (Number, Number) => Number): Vec = {
    require(other.size == size, "Can't perform element-wise operation on vectors of different length")

    other match {
      case otherSparseArray: SparseArrayVector =>
        val zeros_vec_a      = this.sparseVec._2.map(q => (q, Number.zero))
        val zeros_vec_b      = otherSparseArray.sparseVec._2.map(q => (Number.zero, q))
        val combined_indices = this.sparseVec._1 ++ otherSparseArray.sparseVec._1
        val combined_values  = zeros_vec_a ++ zeros_vec_b
        val sorted           = order_by_index((combined_indices, combined_values))

        val map: Map[Int, ((Number, Number), Int)] = Map()
        val test = sorted._2.foldLeft(map)((acc, tuple) => {

          val index = combined_indices(tuple._2)
          if (acc.contains(index)) {

            val value_a   = acc(index)._1._1 + tuple._1._1
            val value_b   = acc(index)._1._2 + tuple._1._2
            val new_tuple = ((value_a, value_b), tuple._2)
            acc + (index -> new_tuple)
          }
          else {
            val data = acc + (index -> tuple)
            data
          }

        })

        val result          = test.map(data => (data._1, op(data._2._1._1, data._2._1._2))).toList
        val filtered_result = result.filter(q => abs(q._2) > SparseArrayVector.epsilon)
        val result_index    = filtered_result.map(k => k._1)
        val result_value    = filtered_result.map(k => k._2)

        SparseArrayVector((result_index, result_value), this.size)

      case vec =>
        //TODO
        throw new UnsupportedOperationException("Coming soon")
    }
  }

  override def foldLeft[B](init: B)(op: (B, Number) => B): B = sparseVec._2.foldLeft(init)(op)

  override def sparse = this

  override def map: Map[Int, Number] = sparseVec._1.zip(sparseVec._2).toMap

  override def nonZeroIndices(epsilon: Number = 1e-20): Iterable[Int] = {
    if (abs(epsilon) >= SparseArrayVector.epsilon) {
      map.keys
    }
    else {
      map.filter {
        case (_, num) => abs(num) > epsilon
      }.keys
    }
  }

  override def nonZeroCount(epsilon: Number = 1e-20): Int = {
    if (abs(epsilon) >= SparseArrayVector.epsilon) {
      this.sparseVec._1.length
    }
    else {
      sparseVec._2.count(abs(_) > epsilon)
    }
  }

  def nonZeroCount(): Int = {
    this.sparseVec._1.length
  }

  def mapValues(op: Number => Number): SparseArrayVector = {

    val result = this.sparseVec._2.map(data => op(data))

    SparseArrayVector((this.sparseVec._1, result), this.size)
  }

  def nonZeroIndices(): List[Int] = {
    this.sparseVec._1
  }

  def zeros_like(dimension: Int): SparseArrayVector = {

    val zero_vec = List.fill(dimension)(Number.zero)
    val indices  = List.range(0, dimension)

    SparseArrayVector((indices, zero_vec), dimension)
  }

  def order_by_index(input: (List[Int], List[(Number, Number)])): (List[Int], List[((Number, Number), Int)]) = {

    var indices = input._1.zipWithIndex
    var values  = input._2.zipWithIndex
    indices = indices.sortBy(x => x._1)

    val order = indices.map(x => x._2)
    values = values.sortWith((a, b) => order.indexOf(a._2) < order.indexOf(b._2))
    (indices.map(x => x._1), values)
  }

}

object SparseArrayVector {

  val epsilon: Number = 1e-20

  def apply(m: Map[Int, Number], size: Int): SparseArrayVector = {
    require(m.size <= size, "The sparse vector contains more elements than its defined size. Impossibru")

    val filtered = m.filter {
      case (_, num) => abs(num) > Sparse.epsilon
    }
    SparseArrayVector((filtered.keys.toList, filtered.values.toList), size)
  }

  def apply(input: Iterator[String], dim: Int): SparseArrayVector = {
    SparseArrayVector(csrFormat(input), dim)
  }

  def csrFormat(input: Iterator[String]): (List[Int], List[Number]) = {
    val col_val_tuples = input.flatMap(f => parseRow(f)).toList
    val csr_values     = col_val_tuples.map(k => k._2)
    val csr_columns    = col_val_tuples.map(k => k._1)
    (csr_columns, csr_values)
  }

  /*
	* changes the text file having form index:value into a list having
	* each of them stored in a tuple
	*/
  def parseRow(row: String): List[(Int, Number)] = {
    val parser = row.split(" ").map(f => f.split(":")).map(f => (f(0).toInt, Number(f(1)))).toList
    parser
  }
}
