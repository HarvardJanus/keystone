package nodes.stats

import breeze.linalg.{DenseVector, DenseMatrix}
import breeze.numerics._
import lineage._
import org.apache.spark.rdd.RDD
import workflow.Transformer

/**
 *  Apply power normalization: z <- sign(z)|z|^{\rho}
 *  with \rho = \frac{1}{2}
 *  This a "signed square root"
 */
object SignedHellingerMapper extends Transformer[DenseVector[Double], DenseVector[Double]] {
  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    signum(in) :* sqrt(abs(in))
  }

  override def saveLineageAndApply(in: RDD[DenseVector[Double]], tag: String): RDD[DenseVector[Double]] = {
    val stamp1 = System.nanoTime()
    val out = in.map(apply)
    out.cache()
    out.count()
    val stamp2 = System.nanoTime()    
    val lineage = AllLineage(in, out, this)
    lineage.saveMapping(tag)
    val stamp3 = System.nanoTime()
    lineage.saveOutput(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(List(Coor(0,0))))
    val stamp4 = System.nanoTime()
    println(s"Transformer $tag: exec: ${(stamp2 - stamp1)/1e9}s, mapping: ${(stamp3-stamp2)/1e9}s, output: ${(stamp4-stamp3)/1e9}s")
    out
  }
}

object BatchSignedHellingerMapper extends Transformer[DenseMatrix[Float], DenseMatrix[Float]] {
  def apply(in: DenseMatrix[Float]): DenseMatrix[Float] = {
    in.map(x => (math.signum(x) * math.sqrt(math.abs(x))).toFloat)
  }
}
