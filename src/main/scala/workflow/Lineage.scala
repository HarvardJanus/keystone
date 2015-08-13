package workflow

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import utils.{MultiLabeledImage, Image, LabeledImage, ImageMetadata}

import java.io._
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.io.Source

/**
 *  Each Lineage corresponds to one transformer
 *  @param inRDD input RDD of the transformer
 *  @param outRDD output RDD of the transformer
 *  @param mappingRDD each item of mappingRDD corresponds to one item in inRDD and one item in outRDD
 *  @param modelRDD related models of transformer, it can be a random seed, a vector or a matrix
 *  @param transformer the transformer itself
 */

abstract class Lineage(modelRDD: Option[_]) extends serializable{
  val path = "Lineage"
  //qForward() and qBackward() methods need implementation, should call to mappingRDD
  def qForward(key: Option[_]) = List((1, 1))
  def qBackward(key: Option[_]) = List((1, 1))
  def save(tag: String)
}

class NarrowLineage(inRDD: RDD[_], outRDD: RDD[_], mappingRDD: RDD[_], transformer: Transformer[_,_], 
  modelRDD: Option[_]) extends Lineage(modelRDD){

  def save(tag: String) = {
    val context = mappingRDD.context
    val rdd = context.parallelize(Seq(transformer), 1)
    rdd.saveAsObjectFile(path+"/"+tag+"/transformer")
    mappingRDD.saveAsObjectFile(path+"/"+tag+"/mappingRDD")
  }
}

class GatherLineage(inSeq: Seq[RDD[_]], outRDD: RDD[_], mapping: TransposeMapping, transformer: GatherTransformer[_], 
  modelRDD: Option[_]) extends Lineage(modelRDD){

  def save(tag: String) = {
    val context = outRDD.context
    val rdd = context.parallelize(Seq(transformer), 1)
    rdd.saveAsObjectFile(path+"/"+tag+"/transformer")
    val mrdd = context.parallelize(Seq(mapping), 1)
    mrdd.saveAsObjectFile(path+"/"+tag+"/mappingRDD")
  }
}

class SampleLineage(inRDD: RDD[_], outRDD: RDD[_], fMapping: RDD[_], bMapping: RDD[_], modelRDD: Option[_]) extends Lineage(modelRDD){
  //This is a temporary solution since ColumnSampler is not a transformer yet
  def save(tag: String) = {
    fMapping.saveAsObjectFile(path+"/"+tag+"/fMappingRDD")
    bMapping.saveAsObjectFile(path+"/"+tag+"/bMappingRDD")
  }
}

object Lineage{
  implicit def intToOption(key: Int): Option[Int] = Some(key)
  implicit def int2DToOption(key: (Int, Int)): Option[(Int, Int)] = Some(key)
  implicit def indexInt2DToOption(key: (Int, (Int, Int))): Option[(Int, (Int, Int))] = Some(key)
}

object OneToOneLineage{
  def apply(in: RDD[_], out:RDD[_], transformer: Transformer[_, _], model: Option[_] = None) = {
    val mapping = in.zip(out).map({
      case (vIn: DenseVector[_], vOut: DenseVector[_]) => {
        new OneToOneMapping(vIn.size, 1, vOut.size, 1, 1)
      }
      case (sIn: Seq[_], vOut: DenseVector[_]) => {
        //sIn should be Seq[DenseVetor[_]]
        val sampleInVector = sIn(0).asInstanceOf[DenseVector[_]]
        new OneToOneMapping(sampleInVector.size, 1, vOut.size, 1, sIn.size)
      }
      case (mIn: DenseMatrix[_], mOut: DenseMatrix[_]) => {
        new OneToOneMapping(mIn.rows, mIn.cols, mOut.rows, mOut.cols, 1)
      }
      case (mIn: DenseMatrix[_], mOut: DenseVector[_]) => {
        new OneToOneMapping(mIn.rows, mIn.cols, mOut.size, 1, 1)
      }
      case (imageIn: MultiLabeledImage, imageOut: Image) => {
        new OneToOneMapping(imageIn.image.flatSize, 1, imageOut.flatSize, 1, 1, Some(imageOut.metadata))
      }
      case (imageIn: Image, imageOut: Image) => {
        new OneToOneMapping(imageIn.flatSize, 1, imageOut.flatSize, 1, 1, Some(imageOut.metadata))
      }
      case _ => None
    })
    new NarrowLineage(in, out, mapping, transformer, model)
  }
}

object AllToOneLineage{
  def apply(in: RDD[_], out:RDD[_], transformer: Transformer[_, _], model: Option[_] = None) = {
    val mapping = in.zip(out).map({
      case (vIn: DenseVector[_], vOut: DenseVector[_]) => {
        new AllToOneMapping(vIn.size, 1, vOut.size, 1)
      }
      case (vIn: DenseVector[_], vOut: Int) => {
        new AllToOneMapping(vIn.size, 1, 1, 1)
      }
      case (vIn: DenseMatrix[_], vOut: DenseMatrix[_]) => {
        new AllToOneMapping(vIn.rows, vIn.cols, vOut.rows, vOut.cols)
      }
      case (vIn: Image, vOut: Image) => {
        new AllToOneMapping(vIn.flatSize, 1, vOut.flatSize, 1, Some(vIn.metadata))
      }
      case _ => None
    })
    new NarrowLineage(in, out, mapping, transformer, model)
  }
}

object LinComLineage{
  def apply[T](in: RDD[_], out:RDD[_], transformer: Transformer[_, _], model: Option[DenseMatrix[T]]) = {
    val m = model.getOrElse(None).asInstanceOf[DenseMatrix[T]]
    val mapping = in.zip(out).map({
      case (sIn: DenseVector[_], sOut: DenseVector[_]) => {
        new LinComMapping(sIn.size, 1, sOut.size, 1, m.rows, m.cols)
      }
      case (sIn: DenseMatrix[_], sOut: DenseMatrix[_]) => {
        new LinComMapping(sIn.rows, sIn.cols, sOut.rows, sOut.cols, m.rows, m.cols)
      }
      case _ => None
    })
    new NarrowLineage(in, out, mapping, transformer, model)
  }
}

object RegionLineage{
  def apply(in: RDD[_], out: RDD[_], ioList: RDD[List[(List[(Int, Int)], List[(Int, Int)])]], 
    transformer: Transformer[_, _], model: Option[_]=None) = {
    val mapping = ioList.map(l => {
      ContourMapping(l)
    })

    new NarrowLineage(in, out, mapping, transformer, model)
    //new SubZeroMapping(ioList, List(in.id), List(out.id))
    //new SimpleMapping(ioList, List(in.id), List(out.id))
  }
}

object GatherLineage{
  def apply[T](in: Seq[RDD[T]], out: RDD[Seq[T]], transformer: GatherTransformer[_], model: Option[_]=None) = {
    val sampleIn = in(0)
    val sampleOut = out.take(1)(0)
    val mapping = TransposeMapping(in.size, sampleIn.count, out.count, sampleOut.size)
    new GatherLineage(in, out, mapping, transformer, model)
  }
}

object SampleLineage{
  def apply(in: RDD[_], out: RDD[_], fList: RDD[List[(Long, Long)]], bList:RDD[(Long, Long)], model: Option[_]=None) = {
    val fMapping = fList.map(x => MiscMapping(x))
    val bMapping = bList.map(x => MiscMapping(x))
    new SampleLineage(in ,out, fMapping, bMapping, model)
  }
}
