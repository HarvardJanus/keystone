package nodes.util

import breeze.linalg._
import org.apache.spark.rdd.RDD
import workflow.Transformer
import workflow._

/**
 * Converts float matrix to a double matrix.
 */
object FloatToDouble extends Transformer[DenseMatrix[Float], DenseMatrix[Double]] {
  def apply(in: DenseMatrix[Float]): DenseMatrix[Double] = convert(in, Double)

  override def saveLineageAndApply(in: RDD[DenseMatrix[Float]], tag: String): RDD[DenseMatrix[Double]] = {
    val out = in.map(apply)
    val lineage = OneToOneLineage(in, out)
    lineage.save(tag)
    println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.getCoor2D((0, 0)))
    out
  }
}
