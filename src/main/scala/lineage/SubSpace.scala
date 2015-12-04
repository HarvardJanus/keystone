package lineage

import breeze.linalg._
import utils.{MultiLabeledImage, Image=>KeystoneImage, LabeledImage, ImageMetadata}

abstract class SubSpace extends Serializable {
  def contain(c: Coor): Boolean
  def expand(): List[Coor]
  def numDim(): Int
}

case class NullSpace() extends SubSpace {
  def contain(c: Coor) = false
  def expand() = List(Coor())
  def numDim() = 0
}

case class Vector(dim: Int) extends SubSpace {
  override def contain(c: Coor) = {
    c match {
      case coor: Coor1D => if (coor.x < dim) true else false
      case _ => {
        require(0==1, {"input is 1-d structure, use 1-d index"})
        false
      }
    }
  }

  override def expand() = (0 until dim).toList.map(Coor(_))
  override def toString(): String = "Vector: "+dim
  override def numDim() = 1
}

case class Matrix(xDim: Int, yDim: Int) extends SubSpace {
  override def contain(c: Coor) = {
    c match {
      case coor: Coor2D => if ((coor.x < xDim)&&(coor.y < yDim)) true else false
      case _ => {
        require(0==1, {"input is 2-d structure, use 2-d index"})
        false
      }
    }
  }

  override def expand() = (for(i <- 0 until xDim; j <- 0 until yDim) yield Coor(i,j)).toList
  override def toString(): String = "Matrix: "+xDim+"x"+yDim
  override def numDim() = 2
}

case class Image(xDim: Int, yDim: Int, cDim: Int) extends SubSpace {
  override def contain(c: Coor) = {
    c match {
      case coor: Coor3D => if ((coor.x < xDim)&&(coor.y < yDim)&&(coor.c < cDim)) true else false
      case _ => {
        require(0==1, {"input is 3-d structure, use 3-d index"})
        false
      }
    }
  }
  override def expand  = (for(i <- 0 until xDim; j <- 0 until yDim; c <- 0 until cDim) yield Coor(i,j,c)).toList
  override def toString(): String =  "Image: "+xDim+"x"+yDim+"x"+cDim
  override def numDim() = 3
}

object SubSpace {
  def apply(): SubSpace = NullSpace()
  def apply(dim: Int): SubSpace = Vector(dim)
  def apply(v: DenseVector[_]): SubSpace = Vector(v.size)

  def apply(xDim: Int, yDim: Int): SubSpace = Matrix(xDim, yDim)
  def apply(m: DenseMatrix[_]): SubSpace = Matrix(m.rows, m.cols)
  
  def apply(xDim: Int, yDim: Int, cDim: Int): SubSpace = Image(xDim, yDim, cDim)
  def apply(meta: ImageMetadata): SubSpace = Image(meta.xDim, meta.yDim, meta.numChannels)
  def apply(kImage: KeystoneImage): SubSpace = SubSpace(kImage.metadata)
  def apply(inImage: MultiLabeledImage): SubSpace = SubSpace(inImage.image.metadata)
}