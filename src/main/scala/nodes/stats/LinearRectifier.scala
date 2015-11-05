package nodes.stats

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import pipelines._
import workflow._
import workflow.Lineage._

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
    val out = in.map(apply)
    out.cache()
    val lineage = OneToOneLineage(in, out, this)
    lineage.save(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(0,0))
    out
  }
}
