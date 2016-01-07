package nodes.util

import breeze.linalg.{DenseMatrix, DenseVector}
import lineage._
import org.apache.spark.rdd.RDD
import workflow.Transformer

/**
 * Flattens a matrix into a vector.
 */
object MatrixVectorizer extends Transformer[DenseMatrix[Double], DenseVector[Double]] {
  def apply(in: DenseMatrix[Double]): DenseVector[Double] = in.toDenseVector

  override def saveLineageAndApply(in: RDD[DenseMatrix[Double]], tag: String): RDD[DenseVector[Double]] = {
    val out = in.map(apply)
    out.cache()
    val lineage = FlattenLineage(in, out, this, 1)
    //lineage.save(tag)
    println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(List(Coor(0,0))))
    out
  }
}
