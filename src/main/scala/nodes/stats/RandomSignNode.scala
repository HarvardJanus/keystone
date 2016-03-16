package nodes.stats

import breeze.linalg._
import breeze.stats.distributions._
import lineage._
import org.apache.spark.rdd.RDD
import workflow.Transformer

/**
 *  A node that takes in DenseVector[Double] and randomly flips
 *  the sign of some of the elements
 */
case class RandomSignNode(signs: DenseVector[Double])
    extends Transformer[DenseVector[Double], DenseVector[Double]] {

  def apply(in: DenseVector[Double]): DenseVector[Double] = in :* signs

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

object RandomSignNode {
  /* Create a random sign node */
  def apply(size: Int, rand: RandBasis = Rand): RandomSignNode = {
    val signs = 2.0*convert(DenseVector.rand(size, Binomial(1, 0.5)(rand)), Double) - 1.0
    new RandomSignNode(signs)
  }
}
