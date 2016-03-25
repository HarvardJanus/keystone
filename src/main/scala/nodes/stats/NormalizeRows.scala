package nodes.stats

import breeze.linalg.{max, sum, DenseVector}
import breeze.numerics._
import lineage._
import org.apache.spark.rdd.RDD
import workflow.Transformer

/**
 * Divides each row by the max of its two-norm and 2.2e-16.
 */
object NormalizeRows extends Transformer[DenseVector[Double], DenseVector[Double]] {
  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    val norm = max(sqrt(sum(pow(in, 2.0))), 2.2e-16)
    in / norm
  }


  override def saveLineageAndApply(in: RDD[DenseVector[Double]], tag: String): RDD[DenseVector[Double]] = {
    val stamp1 = System.nanoTime()
    val out = in.map(apply)
    out.cache()
    out.count()
    val stamp2 = System.nanoTime()
    val lineage = LinComLineage(in, out, this)
    lineage.saveMapping(tag)
    val stamp3 = System.nanoTime()
    //lineage.saveOutput(tag)
    //lineage.saveOutputSmart(tag, stamp3-stamp1)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(List(Coor(0,0))))
    val stamp4 = System.nanoTime()
    println(s"Transformer $tag: exec: ${(stamp2 - stamp1)/1e9}s, mapping: ${(stamp3-stamp2)/1e9}s, output: ${(stamp4-stamp3)/1e9}s")
    out
  }
}