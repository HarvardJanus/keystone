package nodes.stats

import breeze.linalg.DenseVector
import lineage._
import org.apache.spark.rdd.RDD
import pipelines._
import workflow.Transformer

/**
 * This transformer applies a Linear Rectifier,
 * an activation function defined as:
 * f(x) = max({@param maxVal}, x - {@param alpha})
 */
case class LinearRectifier(maxVal: Double = 0.0, alpha: Double = 0.0)
  extends Transformer[DenseVector[Double], DenseVector[Double]] {
  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    in.map(e => math.max(maxVal, e - alpha))
  }

  override def saveLineageAndApply(in: RDD[DenseVector[Double]], tag: String): RDD[DenseVector[Double]] = {
    val stamp1 = System.nanoTime()
    val out = in.map(apply)
    out.cache()
    out.count()
    val stamp2 = System.nanoTime()
    val lineage = IdentityLineage(in, out, this)
    lineage.saveMapping(tag)
    val stamp3 = System.nanoTime()
    lineage.saveOutput(tag)

    val stamp4 = System.nanoTime()
    println(s"Transformer $tag: exec: ${(stamp2 - stamp1)/1e9}s, mapping: ${(stamp3-stamp2)/1e9}s, output: ${(stamp4-stamp3)/1e9}s")
    out
  }
}
