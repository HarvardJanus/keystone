package nodes.images

import org.apache.spark.rdd.RDD
import workflow._
import workflow.Lineage._
import utils.{ImageUtils, Image}

/**
 * Converts an input images to NTSC-standard grayscale.
 */
object GrayScaler extends Transformer[Image,Image] {
  def apply(in: Image): Image = ImageUtils.toGrayScale(in)

  /*override def saveLineageAndApply(in: RDD[Image], tag: String): RDD[Image] = {
    val out = in.map(apply)
    out.cache()
    val lineage = AllToOneLineage(in, out, this)
    lineage.save(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(0,0,0))
    out
  }*/
}
