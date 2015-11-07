package nodes.util

import breeze.linalg._
import org.apache.spark.rdd.RDD
import workflow._
import workflow.Lineage._

/**
 * Converts float matrix to a double matrix.
 */
object FloatToDouble extends Transformer[DenseMatrix[Float], DenseMatrix[Double]] {
  def apply(in: DenseMatrix[Float]): DenseMatrix[Double] = convert(in, Double)

  /*override def saveLineageAndApply(in: RDD[DenseMatrix[Float]], tag: String): RDD[DenseMatrix[Double]] = {
    val out = in.map(apply)
    out.cache()
    val lineage = OneToOneLineage(in, out, this)
    lineage.save(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward((0, 0, 0)))
    out
  }*/
}
