package nodes.util

import breeze.linalg.{DenseVector, argmax}
import lineage._
import org.apache.spark.rdd.RDD
import workflow.Transformer

/**
 * Transformer that returns the index of the largest value in the vector
 */
object MaxClassifier extends Transformer[DenseVector[Double], Int] {
  override def apply(in: DenseVector[Double]): Int = argmax(in)

  override def saveLineageAndApply(in: RDD[DenseVector[Double]], tag: String): RDD[Int] = {
    val stamp1 = System.nanoTime()
    val out = in.map(apply)
    out.cache()
    out.count()
    val stamp2 = System.nanoTime()    
    val lineage = CollapseLineage(in, out, this)
    lineage.saveMapping(tag)
    val stamp3 = System.nanoTime()
    lineage.saveOutput(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(List(Coor(0,0))))
    val stamp4 = System.nanoTime()
    println(s"Transformer $tag: exec: ${(stamp2 - stamp1)/1e9}s, mapping: ${(stamp3-stamp2)/1e9}s, output: ${(stamp4-stamp3)/1e9}s")
    out
  }
}
