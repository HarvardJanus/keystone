package workflow

import breeze.linalg._
import org.apache.spark.rdd.RDD

import utils.{MultiLabeledImage, Image, LabeledImage}

import java.io._
import scala.reflect.ClassTag
import scala.io.Source

trait Lineage extends Serializable{
  def getCoor(key: Int): List[_]
  def save(tag: String) = {
  	val oos = new ObjectOutputStream(new FileOutputStream("Lineage/"+tag))
  	oos.writeObject(this)
  	oos.close
  }
}

case class OneToOneLineage(inRows: Int, inCols: Int, outRows:Int, outCols: Int, 
	seqSize: Int, inRDDs: List[Int], ourRDDs: List[Int]) extends Lineage{
  /** 
   *  The temporary implementation assumes the input and output are Vectors
   *
   */
  def getCoor(key: Int) = {
  	require((key < outRows), {"querying out of boundary of output vector"})
  	seqSize match{
  		case 1 => List(key)
  		case _ => List((key/inRows, key%inRows))
  	}
  }
}

case class AllToOneLineage(inRows: Int, inCols: Int, outRows: Int, outCols: Int, 
	inRDDs: List[Int], ourRDDs: List[Int]) extends Lineage{
	/** 
   *  The temporary implementation assumes the input and output are Vectors
   *
   */
	def getCoor(key: Int): List[Int] = {
		require((key < outRows), {"querying out of boundary of output vector"})
		(0 until inRows).toList
	}
}

case class LinComLineage(inRows: Int, inCols: Int, outRows: Int, outCols: Int,
	modelRows: Int, modelCols: Int, inRDDs: List[Int], ourRDDs: List[Int]) extends Lineage{
	/** 
   *  The temporary implementation assumes the input and output are Vectors
   *
   */
	def getCoor(key: Int): List[(List[Int], List[(Int, Int)])] = {
		require((key < outRows), {"querying out of boundary of output vector"})
		List(((0 until inRows).toList, (0 until inRows).toList.zip(List.fill(modelRows){key})))
	}
}

case class InputLineage(fileRows: Int, offList: List[(String, Int)]) extends Lineage{
	def getCoor(key:Int): List[(String, Int)] = {
	/** 
   *  The temporary implementation assumes the input and output are Vectors
   *
   */
		require((key < offList.size), {"querying out of boundary of output vector"})
		List(offList(key))
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
			case (imageIn: MultiLabeledImage, imageOut: Image) => {
				new OneToOneLineage(imageIn.image.flatSize, 1, imageOut.flatSize, 1, 1, List(in.id), List(out.id))
			}
			case (imageIn: Image, imageOut: Image) => {
				new OneToOneLineage(imageIn.flatSize, 1, imageOut.flatSize, 1, 1, List(in.id), List(out.id))
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
	def apply[T](in: RDD[DenseVector[T]], out:RDD[DenseVector[T]]) = {
		val sampleInVector = in.take(1)(0)
		val sampleOutVector = out.take(1)(0)
		new AllToOneLineage(sampleInVector.size, 1, sampleOutVector.size, 1, List(in.id), List(out.id))
	}

	def apply[T: ClassTag](in: RDD[DenseVector[T]], out:RDD[Int]) = {
		val sampleInVector = in.take(1)(0)
		new AllToOneLineage(sampleInVector.size, 1, 1, 1, List(in.id), List(out.id))
	}
}

object LinComLineage{
	def apply[T](in: RDD[DenseVector[T]], out:RDD[DenseVector[T]], model: DenseMatrix[T]) = {
		val sampleInVector = in.take(1)(0)
		val sampleOutVector = out.take(1)(0)
		new LinComLineage(sampleInVector.size, 1, sampleOutVector.size, 1, model.rows, model.cols, List(in.id), List(out.id))
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
