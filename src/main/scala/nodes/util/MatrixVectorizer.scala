package nodes.util

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import workflow._

/**
 * Flattens a matrix into a vector.
 */
object MatrixVectorizer extends Transformer[DenseMatrix[Double], DenseVector[Double]] {
  def apply(in: DenseMatrix[Double]): DenseVector[Double] = in.toDenseVector
}
