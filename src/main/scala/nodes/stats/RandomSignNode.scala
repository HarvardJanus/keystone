package nodes.stats

import breeze.linalg._
import breeze.stats.distributions._
import org.apache.spark.rdd.RDD
import workflow._
import workflow.Transformer
import workflow.Lineage._

/**
 *  A node that takes in DenseVector[Double] and randomly flips
 *  the sign of some of the elements
 */
case class RandomSignNode(signs: DenseVector[Double])
    extends Transformer[DenseVector[Double], DenseVector[Double]] {

  def apply(in: DenseVector[Double]): DenseVector[Double] = in :* signs

  override def saveLineageAndApply(in: RDD[DenseVector[Double]], tag: String): RDD[DenseVector[Double]] = {
    val out = in.map(apply)
    val lineage = OneToOneLineage(in, out, this, Some(signs))
    lineage.save(tag)
    println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(0))
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
