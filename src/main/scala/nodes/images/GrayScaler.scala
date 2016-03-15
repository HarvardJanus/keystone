package nodes.images

import lineage._
import org.apache.spark.rdd.RDD
import workflow.Transformer
import utils.{ImageUtils, Image}

/**
 * Converts an input images to NTSC-standard grayscale.
 */
object GrayScaler extends Transformer[Image,Image] {
  def apply(in: Image): Image = ImageUtils.toGrayScale(in)

  override def saveLineageAndApply(in: RDD[Image], tag: String): RDD[Image] = {
    val out = in.map(apply)
    out.cache()
    val lineage = CollapseLineage(in, out, this, 2)
    lineage.saveOutput(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(List(Coor(0,0,0))))
    out
  }
}
