package nodes.stats

import breeze.linalg.{max, sum, DenseVector}
import breeze.numerics._
import org.apache.spark.rdd.RDD
import workflow.Transformer
import workflow._
import workflow.Lineage._

/**
 * Divides each row by the max of its two-norm and 2.2e-16.
 */
object NormalizeRows extends Transformer[DenseVector[Double], DenseVector[Double]] {
  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    val norm = max(sqrt(sum(pow(in, 2.0))), 2.2e-16)
    in / norm
  }

  override def saveLineageAndApply(in: RDD[DenseVector[Double]], tag: String): RDD[DenseVector[Double]] = {
    val out = in.map(apply)
    out.cache()
    val lineage = AllToOneLineage(in, out, this)
    lineage.save(tag)
    println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(0, 0))
    out
  }
}