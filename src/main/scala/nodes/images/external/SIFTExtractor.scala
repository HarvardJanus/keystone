package nodes.images.external

import breeze.linalg._
import nodes.images.SIFTExtractorInterface
import org.apache.spark.rdd.RDD
import utils.Image
import utils.external.VLFeat

/**
 * Extracts SIFT Descriptors at dense intervals at multiple scales using the vlfeat C library.
 *
 * @param stepSize Spacing between each sampled descriptor.
 * @param binSize Size of histogram bins for SIFT.
 * @param scales Number of scales at which to extract.
 */
class SIFTExtractor(val stepSize: Int = 3, val binSize: Int = 4, val scales: Int = 4, val scaleStep: Int = 1)
  extends SIFTExtractorInterface {
  @transient lazy val extLib = new VLFeat()

  val descriptorSize = 128
  val descriptorCount = 8192
  /**
   * Extract SIFTs from an image.
   * @param in The input to pass into this pipeline node
   * @return The output for the given input
   */
  def apply(in: Image): DenseMatrix[Float] = {
    var x = Array.ofDim[Double](descriptorCount)
    var y = Array.ofDim[Double](descriptorCount)
    var s = Array.ofDim[Double](descriptorCount)
    val rawDescDataShort = extLib.getSIFTs(in.metadata.xDim, in.metadata.yDim,
      stepSize, binSize, scales, scaleStep, in.getSingleChannelAsFloatArray(), x, y, s)
    val numCols = rawDescDataShort.length/descriptorSize
    val rawDescData = rawDescDataShort.map(s => s.toFloat)
    new DenseMatrix(descriptorSize, numCols, rawDescData)
  }
}

object SIFTExtractor {
  def apply(stepSize: Int = 3, binSize: Int = 4, scales: Int = 4, scaleStep: Int = 1) = {
    new SIFTExtractor(stepSize, binSize, scales, scaleStep)
  }
}
