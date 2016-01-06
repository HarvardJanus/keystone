package nodes.images

import lineage._
import org.apache.spark.rdd.RDD
import workflow.Transformer
import utils.{ImageUtils, Image}

/**
 * Rescales an input image from [0 .. 255] to [0 .. 1]. Works by dividing each pixel by 255.0.
 */
object PixelScaler extends Transformer[Image,Image] {
  def apply(im: Image): Image = {
    ImageUtils.mapPixels(im, _/255.0)
  }

  override def saveLineageAndApply(in: RDD[Image], tag: String): RDD[Image] = {
    val out = in.map(apply)
    out.cache()
    val lineage = IdentityLineage(in, out, this)
    println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(List(Coor(0,0,0,0))))
    out
  }
}