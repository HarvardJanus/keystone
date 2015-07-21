package nodes.images

import org.apache.spark.rdd.RDD
import utils.{MultiLabeledImage, Image, LabeledImage}
import workflow._
import workflow.Transformer
import workflow.Lineage._

/**
 * Extracts a label from a labeled image.
 */
object LabelExtractor extends Transformer[LabeledImage, Int] {
  def apply(in: LabeledImage): Int = in.label
}

/**
 * Extracts an image from a labeled image.
 */
object ImageExtractor extends Transformer[LabeledImage, Image] {
  def apply(in: LabeledImage): Image = in.image
}

/**
 * Extracts a label from a multi-labeled image.
 */
object MultiLabelExtractor extends Transformer[MultiLabeledImage, Array[Int]] {
  override def apply(in: MultiLabeledImage): Array[Int] = in.label
}

/**
 * Extracts an image from a multi-labeled image.
 */
object MultiLabeledImageExtractor extends Transformer[MultiLabeledImage, Image] {
  def apply(in: MultiLabeledImage): Image = in.image

  override def saveLineageAndApply(in: RDD[MultiLabeledImage], tag: String): RDD[Image] = {
    val out = in.map(apply)
    val lineage = OneToOneLineage(in, out)
    lineage.save(tag)
    println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(0))
    out
  }
}
