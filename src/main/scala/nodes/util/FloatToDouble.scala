package nodes.util

import breeze.linalg._
import lineage._
import org.apache.spark.rdd.RDD
import workflow.Transformer

/**
 * Converts float matrix to a double matrix.
 */
object FloatToDouble extends Transformer[DenseMatrix[Float], DenseMatrix[Double]] {
  def apply(in: DenseMatrix[Float]): DenseMatrix[Double] = convert(in, Double)

  override def saveLineageAndApply(in: RDD[DenseMatrix[Float]], tag: String): RDD[DenseMatrix[Double]] = {
    val out = in.map(apply)
    out.cache()
    val lineage = IdentityLineage(in, out, this)
    lineage.saveOutput(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(List(Coor(0,0,0))))
    out
  }
}
