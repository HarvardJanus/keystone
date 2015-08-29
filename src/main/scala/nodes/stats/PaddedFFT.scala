package nodes.stats

import breeze.linalg.DenseVector
import breeze.math.Complex
import org.apache.spark.rdd.RDD
import workflow._
import workflow.Transformer
import workflow.Lineage._

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
    val out = in.map(apply)
    //out.cache()
    val lineage = AllToOneLineage(in, out, this)
    lineage.save(tag)
    println("collecting lineage for Transformer "+this.label+"\t mapping size: "+lineage.qBackward(0, 0).size)
    out
  }

  def nextPositivePowerOfTwo(i : Int) = 1 << (32 - Integer.numberOfLeadingZeros(i - 1))
}
