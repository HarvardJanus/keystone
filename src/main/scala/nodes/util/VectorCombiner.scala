package nodes.util

import breeze.linalg.{DenseMatrix, DenseVector}
import lineage._
import org.apache.spark.rdd.RDD
import workflow.Transformer

import scala.reflect.ClassTag

/**
 * Concats a Seq of DenseVectors into a single DenseVector.
 */
case class VectorCombiner[T : ClassTag]()(implicit zero: breeze.storage.Zero[T])
    extends Transformer[Seq[DenseVector[T]], DenseVector[T]] {
  def apply(in: Seq[DenseVector[T]]): DenseVector[T] = DenseVector.vertcat(in:_*)

  override def saveLineageAndApply(in: RDD[Seq[DenseVector[T]]], tag: String): RDD[DenseVector[T]] = {
    val stamp1 = System.nanoTime()
    val out = in.map(apply)
    out.cache()
    out.count()
    val stamp2 = System.nanoTime()
    val lineage = FlattenLineage(in, out, this)
    lineage.saveMapping(tag)
    val stamp3 = System.nanoTime()
    lineage.saveOutput(tag)

    val stamp4 = System.nanoTime()
    println(s"Transformer $tag: exec: ${(stamp2 - stamp1)/1e9}s, mapping: ${(stamp3-stamp2)/1e9}s, output: ${(stamp4-stamp3)/1e9}s")
    out
  }
}
