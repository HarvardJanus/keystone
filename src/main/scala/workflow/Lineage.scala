package workflow

import breeze.linalg._
import org.apache.spark.rdd.RDD

import utils.{MultiLabeledImage, Image, LabeledImage, ImageMetadata}

import java.io._
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.io.Source

trait Lineage extends Serializable{
	def qForward(key: Option[_]): List[_]
	def qBackward(key: Option[_]): List[_]
  def save(tag: String) = {
  	val oos = new ObjectOutputStream(new FileOutputStream("Lineage/"+tag))
  	oos.writeObject(this)
  	oos.close
  }
}

object Lineage{
	implicit def intToSome(key: Int): Option[Int] = Some(key)
	implicit def int2DToSome(key: (Int, Int)): Option[(Int, Int)] = Some(key)
	implicit def indexInt2DToSome(key: (Int, (Int, Int))): Option[(Int, (Int, Int))] = Some(key)
}

case class OneToOneLineage(inRows: Int, inCols: Int, outRows:Int, outCols: Int, 
	seqSize: Int, inRDDs: List[Int], outRDDs: List[Int], imageMeta: Option[ImageMetadata] = None) extends Lineage{

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i:Int =>{
				require((inCols == 1)&&(outCols == 1)&&(seqSize == 1), {"input is 2-d structure, use 2-d index"})
				require((i < outRows), {"querying out of boundary of output vector"})
				List(i)
			}
			case (i: Int, j: Int) =>{
				require((seqSize > 1)||(inCols > 1), {"input is 1-d structure, use 1-d index"})
				(outCols, seqSize) match {
					case (1, 1) => {
						//This is the case for matrix-to-vector
						require((j*inRows+i < outRows), {"querying out of boundary of input vector"})
						List((j*inRows+i))
					}
					case (_, 1) => {
						//This is the case for matrix-to-matrix
						require((i < outRows) && (j < outCols), {"querying out of boundary of input matrix"})
						List((i, j))
					}
					case (1, _) => {
						require((i < seqSize), {"Sequence index out of bound"})
						require((inRows*i+j < outRows), {"querying out of boundary of input vector sequence"})
						List(inRows * i + j)	
					}
				}
			}
			case _ => List()
		}
	}

	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i:Int =>{
				require((i < outRows), {"querying out of boundary of output vector"})
				(inCols, seqSize) match {
  				case (1, 1) => List(i)
  				case (1, _) => List((i/inRows, i%inRows))
  				case (_, 1) => List((i%inRows, i/inRows))
  			}
			}
			case (i: Int, j: Int) =>{
				require((seqSize == 1) && (outCols > 1), {"output is 1-d structure, use 1-d index"})
				require((i < outRows), {"querying out of boundary of output vector"})
				require((j < outCols), {"querying out of boundary of output vector"})
  			List((i, j))
			}
			case _ => List()
		}
	}
}

case class AllToOneLineage(inRows: Int, inCols: Int, outRows: Int, outCols: Int, 
	inRDDs: List[Int], outRDDs: List[Int], imageMeta: Option[ImageMetadata] = None) extends Lineage{

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i: Int =>{
				require((inCols == 1), {"input is 2-d structure, use 2-d index"})
				imageMeta match {
					//this is the case where input and output are images
					case Some(meta) => {
						require((i < inRows), {"querying out of boundary of input vector"})
						List(i%outRows)
					}
					//this is the case where input and output are 1-d vectors
					case _ => {
						require((i < inRows), {"querying out of boundary of input vector"})
						(0 until outRows).toList
					}
				}
			}
			case (i: Int, j: Int) =>{
				require((inRows > 1)&&(inCols >1), {"input is 1-d structure, use 1-d index"})
				require((i < inRows)&&(j < inCols), {"querying out of boundary of input matrix"})
				val rSeq = for {
					x <- 0 until outRows
					y <- 0 until outCols
				} yield (x, y)
				rSeq.toList
			}
			case _ => List()
		}
	}

	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i: Int =>{
				require((outCols == 1), {"output is 2-d structure, use 2-d index"})
				imageMeta match{
					//this is the case where input and output are images
					case Some(meta) => {
						val xDim = meta.xDim
						val yDim = meta.yDim
						val numChannels = meta.numChannels
						(0 until numChannels).toList.map(c => i+c*xDim*yDim)
					}
					case _ => (0 until inRows).toList
				}
			}
			case (i: Int, j: Int) => {
				require((inCols > 1), {"input is 1-d structure, use 1-d index"})
				require((i < inRows)&&(j < inCols), {"querying out of boundary of input matrix"})
				val rSeq = for {
					i <- 0 until inRows
					j <- 0 until inCols
				} yield (i, j)
				rSeq.toList
			}
			case _ => List()
		}
	}
}

case class LinComLineage(inRows: Int, inCols: Int, outRows: Int, outCols: Int,
	modelRows: Int, modelCols: Int, inRDDs: List[Int], outRDDs: List[Int]) extends Lineage{

	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i: Int =>{
				require((outCols == 1)&&(inCols == 1), {"output is 2-d structure, use 2-d index"})
				require((i < outRows), {"querying out of boundary of output vector"})
				List(((0 until inRows).toList, (0 until inRows).toList.zip(List.fill(modelRows){i})))
			}
			case (i: Int, j: Int) => {
				require((outCols > 1)&&(inCols > 1), {"output is 1-d structure, use 1-d index"})
				require((i < outRows)&&(j < outCols), {"querying out of boundary of output matrix"})
				List((List.fill(inCols){i}.zip((0 until inCols).toList), (0 until modelRows).toList.zip(List.fill(modelRows){j})))
			}
		}
	}

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i: Int =>{
				require((outCols == 1)&&(inCols == 1), {"input is 2-d structure, use 2-d index"})
				require((i < inRows), {"querying out of boundary of input vector"})
				(0 until outRows).toList
			}
			case (i: Int, j: Int) => {
				require((outCols > 1)&&(inCols > 1), {"input is 1-d structure, use 1-d index"})
				require((i < inRows)&&(j < inCols), {"querying out of boundary of input matrix"})
				List.fill(outCols){i}.zip((0 until outCols).toList)
			}
			case _ => List()
		}
	}
}

/*case class InputLineage(fileRows: Int, offList: List[(String, Int)]) extends Lineage{
	def getCoor(key: Int): List[(String, Int)] = {
		require((key < offList.size), {"querying out of boundary of output vector"})
		List(offList(key))
	}

	def getCoor2D(key: (Int, Int)) = List()
}*/

case class ShapeLineage(shapeRDD: RDD[List[Shape]], inRDDs: List[Int], outRDDs: List[Int]) extends Lineage{
	val data = shapeRDD.collect.toList

	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i: Int =>{
				data(i)
			}
			case (i: Int, j: Int) =>{
				require((i < data.size), {"querying out of boundary of output RDD of shape list"})
				val shapeList = data(i)

				require((j < shapeList.size), {"querying out of boundary of shape list"})
				val shape = shapeList(j)
				shape.toCoor
			}
		}
	}

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i: Int =>{
				require((false), {"shape lineage forward query does not support 1-d index"})
				List()
			}
			case (itemID: Int, (i: Int, j: Int)) =>{
				findNearestShape(itemID, i.toDouble, j.toDouble)
			}
		}
	}

	def findNearestShape(itemID: Int, i: Double, j: Double): List[Int] = {
		require((itemID < data.size), {"querying out of boundary of shape RDD"})
		val shapeList = data(itemID)
		//val sortedList = shapeList.sortWith(euclideanDistance(_, i,j) < euclideanDistance(_, i,j))
		val tempList = shapeList.zipWithIndex.map{
			case (s, index) => {
				if (s.inShape(i, j)) index
			}
		}
		tempList.filter(_.isInstanceOf[Int]).asInstanceOf[List[Int]]
	}

	def euclideanDistance(shape: Shape, i: Double, j: Double): Double = {
		(shape.getCenter._1-i)*(shape.getCenter._1-i) + (shape.getCenter._2-j)*(shape.getCenter._2-j)
	}
}

case class FlatMapShapeLineage(shapeRDD: RDD[List[Shape]], inRDDs: List[Int], outRDDs: List[Int]) extends Lineage{
	val data = shapeRDD.collect.toList

	def qBackward(key: Option[_]) = {
		val samplesPerList = data(0).size
		val k = key.getOrElse(null)
		k match {
			case (columnID:Int, index: Int) =>{
				require((columnID < data.size*samplesPerList), {"querying out of boundary of the index"})
				//assume a uniform distribution of vectors from matrices or images
				val matrixIndex = columnID/samplesPerList
				val vectorIndex = columnID%samplesPerList
				List((matrixIndex, (index, data(matrixIndex)(vectorIndex).getCenter._2.toInt)))
			}
			case _ => List()
		}
	}

	def qForward(key: Option[_]) = {
		val samplesPerList = data(0).size
		val k = key.getOrElse(null)
		k match{
			case (matrixID: Int) => {
				require((matrixID < data.size), {"querying out of boundary of the input"})
				data(matrixID)
			}
			case (matrixID: Int, (i: Int, j: Int)) =>{
				require((matrixID < data.size), {"querying out of boundary of the input"})
				val shapeList = data(matrixID)
				val columnList = shapeList.map(l => l.getCenter._2)
				if (columnList.contains(j.toDouble)) List((matrixID*samplesPerList+columnList.indexOf(j.toDouble), i)) else List()
			}
			case _ => List()
		}
	}
}

case class SubZeroLineage(mappingRDD: RDD[List[(List[(Int, Int)], List[(Int, Int)])]], inRDDs: List[Int], outRDDs: List[Int]) extends Lineage{
	val (fIndex, bIndex) = buildIndex(mappingRDD.collect.toList)

	def buildIndex(data: List[List[(List[(Int, Int)], List[(Int, Int)])]]): 
		(List[(Map[(Int, Int), List[String]], Map[String, List[(Int, Int)]])], List[(Map[(Int, Int), String], Map[String, List[(Int, Int)]])]) = {
		val fIndex = data.zipWithIndex.map{
			case (itemList, rddIndex) => {
				var map1: Map[(Int, Int), List[String]] = Map()
				var map2: Map[String, List[(Int, Int)]] = Map()
				itemList.zipWithIndex.map{
					case (mapping, mIndex) => {
						val hValue = rddIndex+"-"+mIndex
						map2 += hValue->mapping._2
						mapping._1.map(t => {
							if(map1.contains(t)){
								map1(t) = map1(t) :+ hValue
							}
							else{
								map1 += t->List(hValue)
							}
						})
					}
				}
				(map1, map2)
			}
		}

		val bIndex = data.zipWithIndex.map{
			case (itemList, rddIndex) => {
				var map1: Map[(Int, Int), String] = Map()
				var map2: Map[String, List[(Int, Int)]] = Map()
				itemList.zipWithIndex.map{
					case (mapping, mIndex) => {
						val hValue = rddIndex+"-"+mIndex
						map2 += hValue->mapping._1
						mapping._2.map(t => map1 += t->hValue)
					}
				}
				(map1, map2)
			}
		}
		return (fIndex, bIndex)
	}

	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case (itemID: Int, (i: Int, j: Int)) =>{
				val (m1, m2) = bIndex(itemID)
				val interKey = m1((i, j))
				m2(interKey)
			}
		}
	}

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case (itemID: Int, (i: Int, j: Int)) =>{
				val (m1, m2) = fIndex(itemID)
				val interKeyList = m1((i, j))
				interKeyList.map(interKey => m2(interKey))
			}
		}
	}
}

case class SimpleLineage(mappingRDD: RDD[List[(List[(Int, Int)], List[(Int, Int)])]], inRDDs: List[Int], outRDDs: List[Int]) extends Lineage{
	val (fIndex, bIndex) = buildIndex(mappingRDD.collect.toList)

	def buildIndex(data: List[List[(List[(Int, Int)], List[(Int, Int)])]]): 
		(List[(Map[(Int, Int), List[List[(Int, Int)]]])], List[(Map[(Int, Int), List[(Int, Int)]])]) = {
		val fIndex = data.map{
			itemList => {
				var map: Map[(Int, Int), List[List[(Int, Int)]]] = Map()
				itemList.map{
					mapping => {
						mapping._1.map(t => {
							if(map.contains(t)){
								map(t) = map(t) :+ mapping._2
							}
							else{
								map += t->List(mapping._2)
							}
						})
					}
				}
				map
			}
		}

		val bIndex = data.map{
			itemList => {
				var map: Map[(Int, Int), List[(Int, Int)]] = Map()
				itemList.map{
					mapping => {
						mapping._2.map(t => map += t->mapping._1)
					}
				}
				map
			}
		}

		(fIndex, bIndex)
	}

	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case (itemID: Int, (i: Int, j: Int)) =>{
				val m = bIndex(itemID)
				m((i, j))
			}
		}
	}

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case (itemID: Int, (i: Int, j: Int)) =>{
				val m = fIndex(itemID)
				m((i, j))
			}
		}
	}
}

case class ContourLineage(mappingRDD: RDD[List[(List[(Int, Int)], List[(Int, Int)])]], inRDDs: List[Int], outRDDs: List[Int]) extends Lineage{
	val (fIndex, bIndex) = buildIndex(mappingRDD.collect.toList)

	def buildIndex(data: List[List[(List[(Int, Int)], List[(Int, Int)])]]): 
		(List[(Map[Shape, Shape])], List[(Map[Shape, Shape])]) = {
		val maps = data.map{
			itemList => {
				var fMap: Map[Shape, Shape] = Map()
				var bMap: Map[Shape, Shape] = Map()
				itemList.map{
					mapping => {
						val xList = mapping._1.map(x => x._1)
						val yList = mapping._1.map(x => x._2)
						val x = xList.sum.toDouble/xList.size
						val y = yList.sum.toDouble/yList.size
						val circle = Circle((x, y), 4)

						val upperLeft = (mapping._2.head._1.toDouble, mapping._2.head._2.toDouble)
						val lowerRight = (mapping._2.last._1.toDouble, mapping._2.last._2.toDouble)
						val square = Square(upperLeft, lowerRight)

						fMap += circle->square
						bMap += square->circle
					}
				}
				(fMap, bMap)
			}
		}

		val fIndex = maps.map(x => x._1)
		val bIndex = maps.map(x => x._2)
		(fIndex, bIndex)
	}

	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case (itemID: Int, (i: Int, j: Int)) =>{
				val m = bIndex(itemID)
				val shapeTuple = m.find(_._1.inShape(i.toDouble, j.toDouble)).getOrElse(null)
				shapeTuple match {
					case t:(Shape, Shape) => t._2.toCoor
					case _ => List()
				}
			}
		}
	}

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case (itemID: Int, (i: Int, j: Int)) =>{
				val m = fIndex(itemID)
				val shapeMap= m.filter(_._1.inShape(i.toDouble, j.toDouble))
				if(shapeMap.isEmpty){
					List()
				}
				else{
					val shapes = shapeMap.values.toList
					shapes.map(x => x.toCoor)
				}
			}
		}
	}
}

object RegionLineage{
	def apply(in: RDD[_], out: RDD[_], ioList: RDD[List[(List[(Int, Int)], List[(Int, Int)])]]) = 
		new ContourLineage(ioList, List(in.id), List(out.id))
		//new SubZeroLineage(ioList, List(in.id), List(out.id))
		//new SimpleLineage(ioList, List(in.id), List(out.id))
}

object ShapeLineage{
	def apply(in: RDD[_], out: RDD[_], shapes: RDD[List[Shape]]) = {
		val sampleIn = in.take(1)(0)
		val sampleOut = out.take(1)(0)
		val lineage = (sampleIn, sampleOut) match {
			case (vIn: Image, vOut: DenseMatrix[_]) => {
				new ShapeLineage(shapes, List(in.id), List(out.id))
			}
			case (vIn: DenseMatrix[_], vOut: DenseVector[_]) =>{
				if(in.count < out.count){
					new FlatMapShapeLineage(shapes, List(in.id), List(out.id))
				}
				else{
					new ShapeLineage(shapes, List(in.id), List(out.id))
				}
			}
			case _ => null
		}
		lineage
	}
}

object OneToOneLineage{
	def apply(in: RDD[_], out:RDD[_]) = {
		val sampleIn = in.take(1)(0)
		val sampleOut = out.take(1)(0)
		val lineage = (sampleIn, sampleOut) match{
			case (vIn: DenseVector[_], vOut: DenseVector[_]) => {
				new OneToOneLineage(vIn.size, 1, vOut.size, 1, 1, List(in.id), List(out.id))
			}
			case (sIn: Seq[_], vOut: DenseVector[_]) => {
				//sIn should be Seq[DenseVetor[_]]
				val sampleInVector = sIn(0).asInstanceOf[DenseVector[_]]
				new OneToOneLineage(sampleInVector.size, 1, vOut.size, 1, sIn.size, List(in.id), List(out.id))
			}
			case (mIn: DenseMatrix[_], mOut: DenseMatrix[_]) => {
				new OneToOneLineage(mIn.rows, mIn.cols, mOut.rows, mOut.cols, 1, List(in.id), List(out.id))
			}
			case (mIn: DenseMatrix[_], mOut: DenseVector[_]) => {
				new OneToOneLineage(mIn.rows, mIn.cols, mOut.size, 1, 1, List(in.id), List(out.id))
			}
			case (imageIn: MultiLabeledImage, imageOut: Image) => {
				new OneToOneLineage(imageIn.image.flatSize, 1, imageOut.flatSize, 1, 1, List(in.id), List(out.id), Some(imageOut.metadata))
			}
			case (imageIn: Image, imageOut: Image) => {
				new OneToOneLineage(imageIn.flatSize, 1, imageOut.flatSize, 1, 1, List(in.id), List(out.id), Some(imageOut.metadata))
			}
			case _ => null
		}
		lineage
	}

	def apply[T](in: Seq[RDD[T]], out:RDD[Seq[T]]) = {
		new OneToOneLineage(0, 0, 0, 0, in.size, in.map(r => r.id).toList, List(out.id))
	}
}

object AllToOneLineage{
	def apply(in: RDD[_], out:RDD[_]) = {
		val sampleIn = in.take(1)(0)
		val sampleOut = out.take(1)(0)
		val lineage = (sampleIn, sampleOut) match{
			case (vIn: DenseVector[_], vOut: DenseVector[_]) => {
				new AllToOneLineage(vIn.size, 1, vOut.size, 1, List(in.id), List(out.id))
			}
			case (vIn: DenseVector[_], vOut: Int) => {
				new AllToOneLineage(vIn.size, 1, 1, 1, List(in.id), List(out.id))
			}
			case (vIn: DenseMatrix[_], vOut: DenseMatrix[_]) => {
				new AllToOneLineage(vIn.rows, vIn.cols, vOut.rows, vOut.cols, List(in.id), List(out.id))
			}
			case (vIn: Image, vOut: Image) => {
				new AllToOneLineage(vIn.flatSize, 1, vOut.flatSize, 1, List(in.id), List(out.id), Some(vIn.metadata))
			}
			case _ => null
		}
		lineage
	}
}

object LinComLineage{
	def apply[T](in: RDD[_], out:RDD[_], model: DenseMatrix[T]) = {
		val sampleIn= in.take(1)(0)
		val sampleOut = out.take(1)(0)
		val lineage = (sampleIn, sampleOut) match {
			case (sIn: DenseVector[_], sOut: DenseVector[_]) => {
				new LinComLineage(sIn.size, 1, sOut.size, 1, model.rows, model.cols, List(in.id), List(out.id))
			}
			case (sIn: DenseMatrix[_], sOut: DenseMatrix[_]) => {
				new LinComLineage(sIn.rows, sIn.cols, sOut.rows, sOut.cols, model.rows, model.cols, List(in.id), List(out.id))
			}
			case _ => null
		}
		lineage
	}
}

/*object InputLineage{
	def apply[T](path: String, out: RDD[DenseVector[T]]) = {
		val outSize = out.count
		val lengths = Source.fromFile(path).getLines.toList.map{l => l.size}
		var offset = 0
		val offsets = lengths.map{ 
			l => {
				val orig = offset
				offset = offset + l
				(path, orig)
			}
		}
		new InputLineage(lengths.size, offsets)
	}
}*/
