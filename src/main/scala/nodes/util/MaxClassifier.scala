package nodes.util

import breeze.linalg.{DenseVector, argmax}
import org.apache.spark.rdd.RDD
import workflow._
import workflow.Lineage._

/**
 * Transformer that returns the index of the largest value in the vector
 */
object MaxClassifier extends Transformer[DenseVector[Double], Int] {
  override def apply(in: DenseVector[Double]): Int = argmax(in)

  /*override def saveLineageAndApply(in: RDD[DenseVector[Double]], tag: String): RDD[Int] = {
    val out = in.map(apply)
    out.cache()
    val lineage = AllToOneLineage(in, out, this)
    lineage.save(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping size: "+lineage.qBackward(0, 0).size)
    out
  }*/
}
