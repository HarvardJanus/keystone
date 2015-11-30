package lineage

import utils.ImageMetadata

abstract class SubSpace extends Serializable

case class Vector(dim: Int) extends SubSpace{
  override def toString(): String = "Vector: "+dim
}

case class Matrix(xDim: Int, yDim: Int) extends SubSpace{
  override def toString(): String = "Matrix: "+xDim+"x"+yDim
}

case class Image(xDim: Int, yDim: Int, cDim: Int) extends SubSpace{
  override def toString(): String =  "Image: "+xDim+"x"+yDim+"x"+cDim 
}

object SubSpace{
  def apply(dim: Int): SubSpace = Vector(dim)
  def apply(xDim: Int, yDim: Int): SubSpace = Matrix(xDim, yDim)
  def apply(xDim: Int, yDim: Int, cDim: Int): SubSpace = Image(xDim, yDim, cDim)
  def apply(meta: ImageMetadata): SubSpace = Image(meta.xDim, meta.yDim, meta.numChannels)
}