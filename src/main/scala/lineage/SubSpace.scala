package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD
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

case class Singularity(value: Int) extends SubSpace {
  def contain(c: Coor) = true
  def expand = List(Coor(0))
  def numDim() = 0
  override def toString(): String = "Singularity: 0"
}

case class Vector(dim: Int) extends SubSpace {
  def contain(c: Coor) = {
    c match {
      case coor: Coor1D => if (coor.x < dim) true else false
      case _ => {
        require(0==1, {"input is 1-d structure, use 1-d index"})
        false
      }
    }
  }

  def expand() = (0 until dim).toList.map(Coor(_))
  def numDim() = 1
  override def toString(): String = "Vector: "+dim
}

case class Matrix(xDim: Int, yDim: Int) extends SubSpace {
  def contain(c: Coor) = {
    c match {
      case coor: Coor2D => if ((coor.x < xDim)&&(coor.y < yDim)) true else false
      case _ => {
        require(0==1, {"input is 2-d structure, use 2-d index"})
        false
      }
    }
  }

  def expand() = (for(i <- 0 until xDim; j <- 0 until yDim) yield Coor(i,j)).toList
  def numDim() = 2
  override def toString(): String = "Matrix: "+xDim+"x"+yDim
}

case class Image(xDim: Int, yDim: Int, cDim: Int) extends SubSpace {
  def contain(c: Coor) = {
    c match {
      case coor: Coor3D => if ((coor.x < xDim)&&(coor.y < yDim)&&(coor.c < cDim)) true else false
      case _ => {
        require(0==1, {"input is 3-d structure, use 3-d index"})
        false
      }
    }
  }
  def expand  = (for(i <- 0 until xDim; j <- 0 until yDim; c <- 0 until cDim) yield Coor(i,j,c)).toList
  def numDim() = 3
  override def toString(): String =  "Image: "+xDim+"x"+yDim+"x"+cDim
}

object SubSpace {
  def apply(): SubSpace = NullSpace()
  def apply(value: Int): SubSpace = Singularity(value)

  def apply(v: DenseVector[_]): SubSpace = Vector(v.size)
  def apply(m: DenseMatrix[_]): SubSpace = Matrix(m.rows, m.cols)

  def apply(meta: ImageMetadata): SubSpace = Image(meta.xDim, meta.yDim, meta.numChannels)
  def apply(kImage: KeystoneImage): SubSpace = SubSpace(kImage.metadata)
  def apply(inImage: MultiLabeledImage): SubSpace = SubSpace(inImage.image.metadata)

  def apply[T](in: Seq[RDD[DenseVector[T]]]): SubSpace = {
    val x = in.size
    val y = in.head.count.toInt
    val z = in.head.first.size
    Image(x, y, z)
  }

  def apply[T](in: RDD[Seq[DenseVector[T]]]): SubSpace = {
    val x = in.count.toInt
    val y = in.first.size
    val z = in.first.head.size
    Image(x, y, z)
  }
}