package workflow

import breeze.linalg._
import org.apache.spark.SparkContext
import utils.{MultiLabeledImage, Image, LabeledImage, ImageMetadata}

trait Metadata extends serializable{
  def size(): Int
}

case class VectorMeta(dim:Int) extends Metadata{
  override def size = dim
  override def toString(): String = "Vector: "+dim
}

case class MatrixMeta(xDim: Int, yDim: Int) extends Metadata{
  override def size = xDim * yDim
  override def toString(): String = "Matrix: "+xDim+"x"+yDim
}

case class ImageMeta(xDim: Int, yDim: Int, cDim: Int) extends Metadata{
  override def size = xDim * yDim * cDim
  override def toString(): String =  "Image: "+xDim+"x"+yDim+"x"+cDim 
}

object Metadata{
  def apply(dim: Int) = VectorMeta(dim)
  def apply(xDim: Int, yDim: Int) = MatrixMeta(xDim, yDim)
  def apply(xDim: Int, yDim: Int, cDim: Int) = ImageMeta(xDim, yDim, cDim)
  def apply(meta: ImageMetadata) = ImageMeta(meta.xDim, meta.yDim, meta.numChannels)
}