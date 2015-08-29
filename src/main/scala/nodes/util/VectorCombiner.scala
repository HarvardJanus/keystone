package nodes.util

import breeze.linalg.{DenseMatrix, DenseVector}
import workflow.Transformer
import org.apache.spark.rdd.RDD
import workflow._
import workflow.Lineage._
import scala.reflect.ClassTag

/**
 * Concats a Seq of DenseVectors into a single DenseVector.
 */
case class VectorCombiner[T : ClassTag]()(implicit zero: breeze.storage.Zero[T])
    extends Transformer[Seq[DenseVector[T]], DenseVector[T]] {
  def apply(in: Seq[DenseVector[T]]): DenseVector[T] = DenseVector.vertcat(in:_*)

  override def saveLineageAndApply(in: RDD[Seq[DenseVector[T]]], tag: String): RDD[DenseVector[T]] = {
    val out = in.map(apply)
    //out.cache()
    val lineage = OneToOneLineage(in, out, this)
    lineage.save(tag)
    println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(0,0))
    out
  }
}
