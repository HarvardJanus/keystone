package nodes.images.external

import breeze.linalg._
import nodes.images.SIFTExtractorInterface
import org.apache.spark.rdd.RDD
import utils.Image
import utils.external.VLFeat
import workflow._
import workflow.Lineage._

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

  /**
   * Extract SIFTs from an image.
   * @param in The input to pass into this pipeline node
   * @return The output for the given input
   */
  def apply(in: Image): DenseMatrix[Float] = {
    val descriptorCount = scales * in.metadata.xDim * in.metadata.yDim / stepSize / stepSize
    var x = Array.ofDim[Double](descriptorCount)
    var y = Array.ofDim[Double](descriptorCount)
    var s = Array.ofDim[Double](descriptorCount)
    val rawDescDataShort = extLib.getSIFTs(in.metadata.xDim, in.metadata.yDim,
      stepSize, binSize, scales, scaleStep, in.getSingleChannelAsFloatArray(), x, y, s)
    val numCols = rawDescDataShort.length/descriptorSize
    val rawDescData = rawDescDataShort.map(s => s.toFloat)
    new DenseMatrix(descriptorSize, numCols, rawDescData)
  }

  override def saveLineageAndApply(in: RDD[Image], tag: String): RDD[DenseMatrix[Float]] = {
    val outRDD = in.zipWithIndex.map{ 
      case (image, id) => {
        val descriptorCount = scales * image.metadata.xDim * image.metadata.yDim / stepSize / stepSize
        var x = Array.ofDim[Double](descriptorCount)
        var y = Array.ofDim[Double](descriptorCount)
        var s = Array.ofDim[Double](descriptorCount)
        val rawDescDataShort = extLib.getSIFTs(image.metadata.xDim, image.metadata.yDim,
          stepSize, binSize, scales, scaleStep, image.getSingleChannelAsFloatArray(), x, y, s)
        val numCols = rawDescDataShort.length/descriptorSize
        val rawDescData = rawDescDataShort.map(s => s.toFloat)

        //below is a new interface for lineage, change back to numCols
        val numSamples = 50 //numCols

        val inList = (0 until numSamples).map(i => Circle((x(i), y(i)), binSize.toDouble)).toList
        val outList = (0 until numSamples).map(i => Square((0.0,i.toDouble), ((descriptorSize-1).toDouble,i.toDouble))).toList

        val outMatrix = new DenseMatrix(descriptorSize, numCols, rawDescData)
        (outMatrix, inList.zip(outList))
      }
    }
    
    val out = outRDD.map(x => x._1)
    out.cache()
    val ioList = outRDD.map(x => x._2)
    ioList.cache()
    val lineage = RegionLineage(in, out, ioList, this)

    lineage.save(tag)
    println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward((0,0,0)))
    out
  }
}

object SIFTExtractor {
  def apply(stepSize: Int = 3, binSize: Int = 4, scales: Int = 4, scaleStep: Int = 1) = {
    new SIFTExtractor(stepSize, binSize, scales, scaleStep)
  }
}
