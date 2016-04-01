package nodes.stats

import breeze.linalg.DenseVector
import breeze.math.Complex
import lineage._
import org.apache.spark.rdd.RDD
import workflow.Transformer

/**
 * This transformer pads input vectors to the nearest power of two,
 * then returns the real values of the first half of the fourier transform on the padded vectors.
 *
 * Goes from vectors of size n to vectors of size nextPositivePowerOfTwo(n)/2
 */
case class PaddedFFT() extends Transformer[DenseVector[Double], DenseVector[Double]] {
  override def apply(in: DenseVector[Double]): DenseVector[Double] = {
    val paddedSize = nextPositivePowerOfTwo(in.length)
    val fft: DenseVector[Complex] = breeze.signal.fourierTr(in.padTo(paddedSize, 0.0).toDenseVector)
    fft(0 until (paddedSize / 2)).map(_.real)
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

  def nextPositivePowerOfTwo(i : Int) = 1 << (32 - Integer.numberOfLeadingZeros(i - 1))
}
