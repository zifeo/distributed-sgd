package epfl.distributed.utils
import scala.collection.mutable.ArrayBuffer
import java.io.{BufferedReader,FileReader }
import scala.io.Source
import scala.collection._

object Read{



	def main(args: Array[String]): Unit = {
		
		
		val data = readData_B("lyrl2004_vectors_test_pt0.dat")
		val labels = readLabels("rcv1-v2.topics.qrels")
    		val joinedData = data.map(row => (row._2,labels.get(row._1)))
    		/*
		val start = System.currentTimeMillis()
		val totalTime = System.currentTimeMillis() - start
   		println("Elapsed time: %1d s".format(totalTime/1000))		 
		*/
		
	}


	// read lines and process one by another
	def readData_B(path :String) : Iterator[(Int,Map[Int,Float])] ={

		var iterator = Source.fromFile(path).getLines
		val parsedInput = iterator.map( x => processInput(x))
		return parsedInput
	}

	// construct a map of rowid to 1 or -1 depending on class value
	def readLabels(path : String) : Map[Int,Int] = {

		val labels = Source.fromFile(path).getLines
		val label_tup = labels.map(line =>{

			val sub_line = line.split(" ")
			val classLabel = sub_line(0)
			val rowID = sub_line(1)
			if (classLabel == "CCAT"){
				(rowID.toInt,1)
			}else{
				(rowID.toInt,-1)
			}			
		}).toMap

		return label_tup

	}

	// extract the row which is the first element
	// then for each row extract the column and value tuples
	def processInput(line : String) : (Int,Map[Int,Float]) ={
		
		var data = line.split(" ")
		val rowID = data(0).toInt
		data = data.slice(2,data.size)
		val tupleValues = data.map( row => {
			
			val temp = row.split(":")
			(temp(0).toInt -> temp(1).toFloat)
		})
		val mappedValues = tupleValues.toMap
		(rowID,mappedValues)
	}


}


