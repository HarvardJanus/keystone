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
    val stamp1 = System.nanoTime()
    val out = in.map(apply)
    out.cache()
    out.count()
    val stamp2 = System.nanoTime()
    val lineage = CollapseLineage(in, out, this, 2)
    lineage.saveMapping(tag)    
    val stamp3 = System.nanoTime()
    lineage.saveOutput(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(List(Coor(0,0,0))))
    val stamp4 = System.nanoTime()
    println(s"Transformer $tag: exec: ${(stamp2 - stamp1)/1e9}s, mapping: ${(stamp3-stamp2)/1e9}s, output: ${(stamp4-stamp3)/1e9}s")
    out
  }
}
