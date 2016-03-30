package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import utils.{Image=>KeystoneImage}
import workflow._

case class TransposeLineage[T](inRDD: Seq[RDD[DenseVector[T]]], outRDD: RDD[Seq[DenseVector[T]]], mapping: TransposeMapping) extends Lineage{
  def qForward(keys: List[Coor]) = mapping.qForward(keys)
  def qBackward(keys: List[Coor]) = mapping.qBackward(keys)
  def saveInput() = {}
  def saveOutput(tag: String) = {}
  def saveOutputSmart(tag: String, duration: Long) = {}
  def saveMapping(tag: String) = {
    val context = outRDD.context
    val rdd = context.parallelize(Seq(mapping), 1)
    rdd.saveAsObjectFile(Lineage.path+"/"+tag+"/mappingRDD")
  }
}

object TransposeLineage{
  def apply[T](inSeq: Seq[RDD[DenseVector[T]]], outRDD: RDD[Seq[DenseVector[T]]], dims: (Int,Int)) = {
    val mapping = TransposeMapping(inSeq, outRDD, dims)
    new TransposeLineage(inSeq, outRDD, mapping)
  }
}