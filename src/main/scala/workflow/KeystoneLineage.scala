package workflow

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import utils.{MultiLabeledImage, Image, LabeledImage, ImageMetadata}

import java.io._
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.io.Source

class KeystoneLineage(inRDD: RDD[_], outRDD: RDD[_], mappingRDD: RDD[_], modelRDD: DenseVector[_], 
	transformer: Transformer[_,_]) extends serializable{

	/*  Each Lineage corresponds to one transformer
	 *  inRDD: input RDD of the transformer
	 *  outRDD: output RDD of the transformer
	 *  mappingRDD: each item of mappingRDD corresponds to one item in inRDD and one item in outRDD
	 *  modelRDD: related models of transformer, it can be a random seed, a vector or a matrix
	 *  transformer: the transformer itself
	 */

	def qForward(key: Option[_]) = List((1, 1))
	def qBackward(key: Option[_]) = List((1, 1))

  def save(tag: String) = {
  	//need to save RDD in each directory
  	val path = "Lineage"
  	val context = mappingRDD.context
  	val rdd = context.parallelize(Seq(transformer), 1)
  	rdd.saveAsObjectFile(path+"/"+tag+"/transformer")
  	mappingRDD.saveAsObjectFile(path+"/"+tag+"/mappingRDD")
  }
}

object KeystoneLineage{
	implicit def intToOption(key: Int): Option[Int] = Some(key)
	implicit def int2DToOption(key: (Int, Int)): Option[(Int, Int)] = Some(key)
	implicit def indexInt2DToOption(key: (Int, (Int, Int))): Option[(Int, (Int, Int))] = Some(key)
}

object OneToOneKLineage{
	def apply(in: RDD[_], out:RDD[_], model: DenseVector[_], transformer: Transformer[_, _]) = {
		val mapping = in.zip(out).map({
			case (vIn: DenseVector[_], vOut: DenseVector[_]) => {
				new OneToOneMapping(vIn.size, 1, vOut.size, 1, 1, List(in.id), List(out.id))
			}
			case _ => null
		})
		new KeystoneLineage(in, out, mapping, model, transformer)
	}
}
