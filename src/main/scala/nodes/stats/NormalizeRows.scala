package nodes.stats

import breeze.linalg.{max, sum, DenseVector}
import breeze.numerics._
import org.apache.spark.rdd.RDD
import workflow._

/**
 * Divides each row by the max of its two-norm and 2.2e-16.
 */
object NormalizeRows extends Transformer[DenseVector[Double], DenseVector[Double]] {
  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    val norm = max(sqrt(sum(pow(in, 2.0))), 2.2e-16)
    in / norm
  }
}