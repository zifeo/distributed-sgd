package epfl.distributed.math

import spire.math._

case class SparseArrayVector(sparseVec: (List[Int], List[Number]), size: Int) extends Vec {

  override def apply(idx: Int): Number = sparseVec._1.find(_ == idx).map(sparseVec._2(_)).getOrElse(throw new IndexOutOfBoundsException)

  def elementWiseOp(other: Vec, op: (Number, Number) => Number): Vec = {
    require(other.size == size, "Can't perform element-wise operation on vectors of different length")

    other match {
      case otherSparseArray: SparseArrayVector =>
        
		val zipped_sequenceA = this.sparseVec._1 zip this.sparseVec._2
		val zipped_sequenceB = otherSparseArray.sparseVec._1 zip otherSparseArray.sparseVec._2
		val sortedA = sort(zipped_sequenceA)
		val sortedB = sort(zipped_sequenceB)
		val result = merge(sortedA,sortedB)

		val sum = result.foldLeft( List[(Int,Number)]() )( (acc,data) => {

			if (acc.isEmpty ) {
				data :: acc
			}else{
				val lastElement = acc.last
				if( lastElement._1 == data._1 ){
					acc.updated(acc.size - 1,(lastElement._1,op(lastElement._2,data._2)) )
				}else{
					data :: acc
				}
			}			
		})		

		val res = sum.unzip

		val indices = res._1
		val values = res._2

		SparseArrayVector((indices,values), this.size)

      case vec =>
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
      map.collect {
        case (idx, num) if abs(num) > epsilon => idx
      }
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

  def mapValues(op: Number => Number): SparseArrayVector = {
    val result = this.sparseVec._2.map(data => op(data))
    SparseArrayVector((this.sparseVec._1, result), this.size)
  }


  private[this] def merge(left: List[(Int,Number)], right: List[(Int,Number)]): List[(Int,Number)] =  {
	
	(left, right) match {
		case (_, Nil) => left
		case (Nil, _) => right
		case(leftHead :: leftTail, rightHead :: rightTail) =>
			if (leftHead._1 < rightHead._1) leftHead::merge(leftTail, right)
			else rightHead :: merge(left, rightTail)
	}
  }


  private[this] def sort(input : List[(Int,Number)]) : List[(Int,Number)] = {
	scala.util.Sorting.stableSort(input,(e1:(Int,Number),e2:(Int,Number)) => e1._1 < e2._1)
	input
  }

}

object SparseArrayVector {

  val epsilon: Number = 1e-20

  def apply(m: Map[Int, Number], size: Int): SparseArrayVector = {
    require(m.size <= size, "The sparse vector contains more elements than its defined size. Impossibru")

    val filtered = m.filter {
      case (_, num) => abs(num) > epsilon
    }
    SparseArrayVector((filtered.keys.toList, filtered.values.toList), size)
  }

  def apply[A: Numeric](m: Map[Int, A], size: Int): SparseArrayVector = {
    val num = implicitly[Numeric[A]]
    apply(m.mapValues(num.toNumber), size)
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

   def parseRow(row: String): List[(Int, Number)] = {
    val parser = row.split(" ").map(f => f.split(":")).map(f => (f(0).toInt, Number(f(1)))).toList
    parser
  }
  
  def zeros(size: Int): SparseArrayVector = SparseArrayVector((List(), List()), size)
}
