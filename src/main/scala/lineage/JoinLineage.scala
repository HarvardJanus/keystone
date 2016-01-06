package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
import utils.{Image=>KeystoneImage}
import workflow._

object JoinLineage{
  def apply[T](inSeq: Seq[RDD[DenseVector[T]]], outRDD:RDD[Seq[DenseVector[T]]], transformer: Transformer[_, _]) = {
    val mapping = JoinMapping(inSeq, outRDD)
    new TransposeLineage(inSeq, outRDD, mapping, transformer)
  }
}