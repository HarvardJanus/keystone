package lineage

import breeze.linalg._
import org.apache.spark.rdd.RDD

case class TransposeMapping(inSpace: SubSpace, outSpace: SubSpace, dims: (Int, Int)) extends Mapping(inSpace, outSpace) {
  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    (inSpace, outSpace) match {
      case (in: Image, out: Image) => keys.map(key => {
        val k3d = key.asInstanceOf[Coor3D]
        dims match{
          case (0,1) => Coor(k3d.y, k3d.x, k3d.c)
          case (0,2) => Coor(k3d.c, k3d.y, k3d.x)
          case (1,2) => Coor(k3d.x, k3d.c, k3d.y)
        }  
      })
    }
  }

  def qBackward(keys: List[Coor]) = {
    val flag = keys.map(k => outSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    (inSpace, outSpace) match {
      case (in: Image, out: Image) => keys.map(key => {
        val k3d = key.asInstanceOf[Coor3D]
        dims match{
          case (0,1) => Coor(k3d.y, k3d.x, k3d.c)
          case (0,2) => Coor(k3d.c, k3d.y, k3d.x)
          case (1,2) => Coor(k3d.x, k3d.c, k3d.y)
        }  
      })
    }
  }
}

object TransposeMapping{
  def apply[T](inSR: Seq[RDD[DenseVector[T]]], outRS: RDD[Seq[DenseVector[T]]], dims: (Int, Int)) = 
    new TransposeMapping(SubSpace(inSR), SubSpace(outRS), dims)
}