package nodes.images

import org.apache.spark.rdd.RDD
import workflow.Transformer
import workflow._
import utils.{ImageUtils, Image}
import workflow.KeystoneLineage._

/**
 * Converts an input images to NTSC-standard grayscale.
 */
object GrayScaler extends Transformer[Image,Image] {
  def apply(in: Image): Image = ImageUtils.toGrayScale(in)

  override def saveLineageAndApply(in: RDD[Image], tag: String): RDD[Image] = {
    val out = in.map(apply)
    val lineage = AllToOneKLineage(in, out, this)
    lineage.save(tag)
    println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(0))
    out
  }
}
