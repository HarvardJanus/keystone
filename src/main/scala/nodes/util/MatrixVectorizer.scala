package nodes.util

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import workflow._
import workflow.Lineage._

/**
 * Flattens a matrix into a vector.
 */
object MatrixVectorizer extends Transformer[DenseMatrix[Double], DenseVector[Double]] {
  def apply(in: DenseMatrix[Double]): DenseVector[Double] = in.toDenseVector

  /*override def saveLineageAndApply(in: RDD[DenseMatrix[Double]], tag: String): RDD[DenseVector[Double]] = {
    val out = in.map(apply)
    out.cache()
    val lineage = OneToOneLineage(in, out, this)
    lineage.save(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(0,0))
    out
  }*/
}
