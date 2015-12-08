package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD

case class JoinMapping(inSpace: SubSpace, outSpace: SubSpace, dim: Int) extends Mapping {
  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    (inSpace, outSpace) match {
      case (in: Image, out: Image) => keys.map(key => {
        val k3d = key.asInstanceOf[Coor3D]
        Coor(k3d.y, k3d.x, k3d.c)
        })
    }
  }

  def qBackward(keys: List[Coor]) = {
    val flag = keys.map(k => outSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    (inSpace, outSpace) match {
      case (in: Image, out: Image) => keys.map(key => {
        val k3d = key.asInstanceOf[Coor3D]
        Coor(k3d.y, k3d.x, k3d.c)
      })
    }
  }
}

object JoinMapping{
  def apply[T](inSR: Seq[RDD[DenseVector[T]]], outRS: RDD[Seq[DenseVector[T]]]) = 
    new JoinMapping(SubSpace(inSR), SubSpace(outRS), 1)
}