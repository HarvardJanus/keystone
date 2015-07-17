package workflow

import breeze.linalg._
import org.apache.spark.rdd.RDD

import utils.{MultiLabeledImage, Image, LabeledImage, ImageMetadata}

import java.io._
import scala.reflect.ClassTag
import scala.io.Source

trait Lineage extends Serializable{
  def getCoor(key: Int): List[_]
  def getCoor2D(key: (Int, Int)): List[_]
  def save(tag: String) = {
  	val oos = new ObjectOutputStream(new FileOutputStream("Lineage/"+tag))
  	oos.writeObject(this)
  	oos.close
  }
}

case class OneToOneLineage(inRows: Int, inCols: Int, outRows:Int, outCols: Int, 
	seqSize: Int, inRDDs: List[Int], outRDDs: List[Int], imageMeta: Option[ImageMetadata] = None) extends Lineage{
  /** 
   *  The temporary implementation assumes the input and output are Vectors
   *
   */

  def getCoor(key: Int) = {
  	require((key < outRows), {"querying out of boundary of output vector"})
  	(inCols, seqSize) match {
  		case (1, 1) => List(key)
  		case (1, _) => List((key/inRows, key%inRows))
  		case (_, 1) => List((key%inRows, key/inRows))
  	}
  }

  def getCoor2D(key: (Int, Int)) = {
  	val r = key._1
		val c = key._2
		require((r < outRows), {"querying out of boundary of output vector"})
		require((c < outCols), {"querying out of boundary of output vector"})
  	seqSize match {
  		case 1 => List(key)
  		case _ => List()
  	}
  }
}

case class AllToOneLineage(inRows: Int, inCols: Int, outRows: Int, outCols: Int, 
	inRDDs: List[Int], outRDDs: List[Int], imageMeta: Option[ImageMetadata] = None) extends Lineage{
	/** 
   *  The temporary implementation assumes the input and output are Vectors
   *
   */
	def getCoor(key: Int) = {
		require((key < outRows), {"querying out of boundary of output vector"})
		val rList = imageMeta match {
			case Some(meta) => {
				val xDim = meta.xDim
				val yDim = meta.yDim
				val numChannels = meta.numChannels
				(0 until numChannels).toList.map(c => key+c*xDim*yDim)
			}
			case None => {
				(0 until inRows).toList
			}
		}
		rList
	}

	def getCoor2D(key: (Int, Int)) = {
		val r = key._1
		val c = key._2
		require((r < outRows), {"querying out of boundary of output matrix"})
		require((c < outCols), {"querying out of boundary of output matrix"})
		val rSeq = for {
			i <- 0 until inRows
			j <- 0 until inCols
		} yield (i, j)
		rSeq.toList
	}
}

case class LinComLineage(inRows: Int, inCols: Int, outRows: Int, outCols: Int,
	modelRows: Int, modelCols: Int, inRDDs: List[Int], outRDDs: List[Int]) extends Lineage{
	/** 
   *  The temporary implementation assumes the input and output are Vectors
   *
   */
	def getCoor(key: Int): List[(List[Int], List[(Int, Int)])] = {
		require((key < outRows), {"querying out of boundary of output vector"})
		require((outCols == 1), {"output is a matrix, try use getCoor2D"})
		List(((0 until inRows).toList, (0 until inRows).toList.zip(List.fill(modelRows){key})))
	}

	def getCoor2D(key: (Int, Int)): List[(List[(Int, Int)], List[(Int, Int)])] = {
		val r = key._1
		val c = key._2
		require((r < outRows), {"querying out of boundary of output vector"})
		require((c < outCols), {"querying out of boundary of output vector"})
		List((List.fill(inCols){r}.zip((0 until inCols).toList), (0 until modelRows).toList.zip(List.fill(modelRows){c})))
	}
}

case class InputLineage(fileRows: Int, offList: List[(String, Int)]) extends Lineage{
	def getCoor(key: Int): List[(String, Int)] = {
	/** 
   *  The temporary implementation assumes the input and output are Vectors
   *
   */
		require((key < offList.size), {"querying out of boundary of output vector"})
		List(offList(key))
	}

	def getCoor2D(key: (Int, Int)) = List()
}

case class ShapeLineage(shapeRDD: RDD[List[Shape]], inRDDs: List[Int], outRDDs: List[Int]) extends Lineage{
	val data = shapeRDD.collect.toList
	def getCoor(key: Int): List[Shape] = {
		data(key)
	}
	def getCoor2D(key: (Int, Int)): List[Shape] = {
		val itemID = key._1
		val index = key._2

		require((itemID < data.size), {"querying out of boundary of output RDD of shape list"})
		val shapeList = data(itemID)

		require((index < shapeList.size), {"querying out of boundary of shape list"})
		val shape = shapeList(index)
		List(shape)
	}
}

object ShapeLineage{
	def apply(in: RDD[_], out: RDD[_], shapes: RDD[List[Shape]]) = {
		val sampleIn = in.take(1)(0)
		val sampleOut = out.take(1)(0)
		val lineage = (sampleIn, sampleOut) match {
			case (vIn: Image, vOut: DenseMatrix[_]) => {
				new ShapeLineage(shapes, List(in.id), List(out.id))
			}
			case _ => null
		}
		lineage
	}
}

object OneToOneLineage{
	/*def apply[T](in: RDD[DenseVector[T]], out:RDD[DenseVector[T]]) = {
		val sampleInVector = in.take(1)(0)
		val sampleOutVector = out.take(1)(0)
		new OneToOneLineage(sampleInVector.size, 1, sampleOutVector.size, 1, 1, List(in.id), List(out.id))
	}*/
	def apply(in: RDD[_], out:RDD[_]) = {
		val sampleIn = in.take(1)(0)
		val sampleOut = out.take(1)(0)
		val lineage = (sampleIn, sampleOut) match{
			case (vIn: DenseVector[_], vOut: DenseVector[_]) => {
				new OneToOneLineage(vIn.size, 1, vOut.size, 1, 1, List(in.id), List(out.id))
			}
			case (sIn: Seq[DenseVector[_]], vOut: DenseVector[_]) => {
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

	/*def apply[T: ClassTag](in: RDD[Seq[DenseVector[T]]], out:RDD[DenseVector[T]]) = {
		val sampleInSec = in.take(1)(0)
		val sampleInVector = sampleInSec(0)
		val sampleOutVector = out.take(1)(0)
		new OneToOneLineage(sampleInVector.size, 1, sampleOutVector.size, 1, sampleInSec.size, List(in.id), List(out.id))
	}*/

	def apply[T](in: Seq[RDD[T]], out:RDD[Seq[T]]) = {
		new OneToOneLineage(0, 0, 0, 0, in.size, in.map(r => r.id).toList, List(out.id))
	}

	/*def apply(in: RDD[MultiLabeledImage], out: RDD[Image]) = {
		val sampleInMultiLabeledImage = in.take(1)(0)
		val sampleOutImage = out.take(1)(0)
		new OneToOneLineage(sampleInMultiLabeledImage.image.flatSize, 1, sampleOutImage.flatSize, 1, 1, List(in.id), List(out.id))
	}*/
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
	/*def apply[T](in: RDD[DenseVector[T]], out:RDD[DenseVector[T]], model: DenseMatrix[T]) = {
		val sampleInVector = in.take(1)(0)
		val sampleOutVector = out.take(1)(0)
		new LinComLineage(sampleInVector.size, 1, sampleOutVector.size, 1, model.rows, model.cols, List(in.id), List(out.id))
	}*/
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

object InputLineage{
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
}
