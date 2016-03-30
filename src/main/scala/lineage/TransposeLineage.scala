package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import utils.{Image=>KeystoneImage}
import workflow._

case class TransposeLineage[T](inRDD: Seq[RDD[DenseVector[T]]], outRDD: RDD[Seq[DenseVector[T]]], mapping: TransposeMapping, transformer: Transformer[_,_]) extends Lineage{
  def qForward(keys: List[Coor]) = mapping.qForward(keys)
  def qBackward(keys: List[Coor]) = mapping.qBackward(keys)
  def saveInput() = {}
  def saveOutput(tag: String) = {}
  def saveOutputSmart(tag: String, duration: Long) = {}
  def saveMapping(tag: String) = {}
}

object TransposeLineage{
  def apply[T](inSeq: Seq[RDD[DenseVector[T]]], outRDD: RDD[Seq[DenseVector[T]]], dims: (Int,Int), transformer: Transformer[_, _]) = {
    val mapping = TransposeMapping(inSeq, outRDD, dims)
    new TransposeLineage(inSeq, outRDD, mapping, transformer)
  }
}