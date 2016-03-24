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
    val stamp1 = System.nanoTime()
    val out = in.map(apply)
    out.cache()
    out.count()
    val stamp2 = System.nanoTime()
    val lineage = FlattenLineage(in, out, this, 1)
    lineage.saveMapping(tag)
    val stamp3 = System.nanoTime()
    //lineage.saveOutput(tag)
    lineage.saveOutputSmart(tag, stamp3-stamp1)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(List(Coor(0,0))))
    val stamp4 = System.nanoTime()
    println(s"Transformer $tag: exec: ${(stamp2 - stamp1)/1e9}s, mapping: ${(stamp3-stamp2)/1e9}s, output: ${(stamp4-stamp3)/1e9}s")
    out
  }
}
