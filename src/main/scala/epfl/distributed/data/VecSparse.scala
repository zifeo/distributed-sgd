import scala.io.Source
import java.io.File
import java.io.PrintWriter



class SparseVector {

	private var sparse_vec : (List[Int],List[Float]) = (List(),List())
	private var dimension : Int = 0

	def this(input : Iterator[String],dim : Int) {
		
		this()
		this.sparse_vec = csrFormat(input)
		this.dimension = dim

	}


	def csrFormat(input : Iterator[String]) : (List[Int],List[Float]) ={
    
		val col_val_tuples = input.flatMap(f => parseRow(f)).toList
     		val csr_values = col_val_tuples.map (k => k._2)
     		val csr_columns = col_val_tuples.map( k => k._1)
		return (csr_columns,csr_values)
     
  	}

	/*
	* changes the text file having form index:value into a list having 
	* each of them stored in a tuple
	*/
	def parseRow(row : String) : List[(Int,Float)] = {
		val parser = row.split(" ").map(f => f.split(":")).map(f => (f(0).toInt,f(1).toFloat)).toList
		parser	
	}


	def element_wise_op(other : SparseVector, op : (Float,Float) => Float ) : SparseVector = {
    
	
		val zeros_vec_a = this.sparse_vec._2.map( q => (q,0.toFloat))
		val zeros_vec_b = other.sparse_vec._2.map( q => (0.toFloat,q))
		val combined_indices = this.sparse_vec._1  ++ other.sparse_vec._1 
		val combined_values  = zeros_vec_a   ++ zeros_vec_b 
		val sorted = order_by_index((combined_indices,combined_values))
		var list  : List[((Float,Float),Int)] = List()

		var map : Map[Int , ((Float,Float),Int)] = Map()
		val test = sorted._2.foldLeft(map) ( (acc,tuple) => {

		val index = combined_indices(tuple._2)
			if(acc.contains(index) ) {

				val value_a =  acc(index)._1._1 + tuple._1._1
				val value_b =  acc(index)._1._2 + tuple._1._2
				val new_tuple = ((value_a,value_b),tuple._2)
				acc + ( index -> new_tuple )
			}
			else{
				val data = acc + ( index -> tuple )
				data
			}

		})

		val result = test.map ( data => (data._1,op(data._2._1._1,data._2._1._2))).toList
		val filtered_result = result.filter( q => q._2!=0.toFloat)
		val result_index = filtered_result.map(k => k._1)
		val result_value = filtered_result.map(k => k._2)

		var return_type = new SparseVector
		return_type.sparse_vec = (result_index,result_value)
		return_type.dimension = this.dimension
		
		return return_type	
		//return SparseVector((result_index,result_value),this.dimension)
       
  	}
  
	def nonZeroCount() : Int = {
		return this.sparse_vec._1.length
	}


	def mapValues(op: Float => Float) : SparseVector  = {

		val result = this.sparse_vec._2.map(data => op(data) )
		var return_type = new SparseVector
		return_type.sparse_vec = (this.sparse_vec._1,result)
		return_type.dimension = this.dimension
		
		return return_type 
	}

	def nonZeroIndices(): List[Int] = {
		return this.sparse_vec._1
	}

	def zeros_like(dimension : Int) : SparseVector  = {

		val zero_vec = List.fill(dimension)(0.toFloat)		
		val indices = List.range(0,dimension)
		
		var return_type = new SparseVector
		return_type.sparse_vec = (indices,zero_vec)
		return_type.dimension = dimension
		
		return return_type 

	}

	 def order_by_index(input : (List[Int],List[(Float,Float)]) ) :  (List[Int],List[((Float,Float),Int)]) ={
	
		var indices = input._1.zipWithIndex
		var values =  input._2.zipWithIndex
		indices = indices.sortBy( x => x._1)
		val order = indices.map( x => x._2)
		values = values.sortWith( (a,b) => order.indexOf(a._2) < order.indexOf(b._2) )
		return (indices.map(x => x._1),values)
  	}
  
}
