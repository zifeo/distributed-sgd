package epfl.distributed.math

trait VecAPI {

  def elementWiseOp(other: Vec, op: (Float, Float) => Float): Vec

  def mapValues(op: Float => Float): Vec

  def foldLeft[B](init: B)(op: (B, Float) => B): B

  def +(other: Vec): Vec = elementWiseOp(other, _ + _)

  def +(scalar: Float): Vec = mapValues(_ + scalar)

  def -(other: Vec): Vec = elementWiseOp(other, _ - _)

  def -(scalar: Float): Vec = mapValues(_ - scalar)

  def *(other: Vec): Vec = elementWiseOp(other, _ * _)

  def *(scalar: Float): Vec = mapValues(_ * scalar)

  def /(scalar: Float): Vec = mapValues(_ / scalar)

  def pow(scalar: Float): Vec = mapValues(math.pow(_, scalar).toFloat)

  def sum: Float = foldLeft(0.toFloat)(_ + _)

  def normSquared: Float = math.pow(foldLeft(0.toFloat)(_ + _), 2).toFloat
  def norm: Float        = math.pow(normSquared, 0.5).toFloat

  def dot(other: Vec): Number = {
    (this * other).sum
  }

}

class SparseArrayVector {

  private var sparseVec: (List[Int], List[Float]) = (List(), List())
  private var dimension: Int                       = 0

  def this(input: Iterator[String], dim: Int) {
    this()
    this.sparseVec = csrFormat(input)
    this.dimension = dim
  }

  def csrFormat(input: Iterator[String]): (List[Int], List[Float]) = {
    val col_val_tuples = input.flatMap(f => parseRow(f)).toList
    val csr_values     = col_val_tuples.map(k => k._2)
    val csr_columns    = col_val_tuples.map(k => k._1)
    (csr_columns, csr_values)
  }

  /*
	* changes the text file having form index:value into a list having
	* each of them stored in a tuple
	*/
  def parseRow(row: String): List[(Int, Float)] = {
    val parser = row.split(" ").map(f => f.split(":")).map(f => (f(0).toInt, f(1).toFloat)).toList
    parser
  }

  def element_wise_op(other: SparseArrayVector, op: (Float, Float) => Float): SparseArrayVector = {

    val zeros_vec_a      = this.sparseVec._2.map(q => (q, 0.toFloat))
    val zeros_vec_b      = other.sparseVec._2.map(q => (0.toFloat, q))
    val combined_indices = this.sparseVec._1 ++ other.sparseVec._1
    val combined_values  = zeros_vec_a ++ zeros_vec_b
    val sorted           = order_by_index((combined_indices, combined_values))

    val map: Map[Int, ((Float, Float), Int)] = Map()
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
    val filtered_result = result.filter(q => q._2 != 0.toFloat)
    val result_index    = filtered_result.map(k => k._1)
    val result_value    = filtered_result.map(k => k._2)

    val returnType = new SparseArrayVector
    returnType.sparseVec = (result_index, result_value)
    returnType.dimension = this.dimension

    //SparseVector((result_index,result_value),this.dimension)
    returnType
  }

  def nonZeroCount(): Int = {
    this.sparseVec._1.length
  }

  def mapValues(op: Float => Float): SparseArrayVector = {

    val result      = this.sparseVec._2.map(data => op(data))
    val returnType = new SparseArrayVector
    returnType.sparseVec = (this.sparseVec._1, result)
    returnType.dimension = this.dimension

    returnType
  }

  def nonZeroIndices(): List[Int] = {
    this.sparseVec._1
  }

  def zeros_like(dimension: Int): SparseArrayVector = {

    val zero_vec = List.fill(dimension)(0.toFloat)
    val indices  = List.range(0, dimension)

    val returnType = new SparseArrayVector
    returnType.sparseVec = (indices, zero_vec)
    returnType.dimension = dimension

    returnType
  }

  def order_by_index(input: (List[Int], List[(Float, Float)])): (List[Int], List[((Float, Float), Int)]) = {

    var indices = input._1.zipWithIndex
    var values  = input._2.zipWithIndex
    indices = indices.sortBy(x => x._1)

    val order = indices.map(x => x._2)
    values = values.sortWith((a, b) => order.indexOf(a._2) < order.indexOf(b._2))
    (indices.map(x => x._1), values)
  }

}
