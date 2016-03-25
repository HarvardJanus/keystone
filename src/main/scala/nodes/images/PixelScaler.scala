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
    val stamp1 = System.nanoTime()
    val out = in.map(apply)
    out.cache()
    out.count()
    val stamp2 = System.nanoTime()
    val lineage = IdentityLineage(in, out, this)
    lineage.saveMapping(tag)    
    val stamp3 = System.nanoTime()
    //lineage.saveOutput(tag)
    //lineage.saveOutputSmart(tag, stamp3-stamp1)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(List(Coor(0,0,0,0))))
    val stamp4 = System.nanoTime()
    println(s"Transformer $tag: exec: ${(stamp2 - stamp1)/1e9}s, mapping: ${(stamp3-stamp2)/1e9}s, output: ${(stamp4-stamp3)/1e9}s")
    out
  }
}