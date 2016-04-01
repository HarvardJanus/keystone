package lineage

import breeze.linalg._
import utils.{MultiLabeledImage, Image=>KeystoneImage, ImageMetadata, LabeledImage}

/*
 *  dim is the dimension that breaks in the flattening
 */
case class FlattenMapping(inSpace: SubSpace, outSpace: SubSpace, dim: Int) extends Mapping(inSpace, outSpace){
  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    Mapping.queryOptimization match{
      case true => {
        //println("flatten mapping forward optimization path")
        val rule = FlattenForwardQueryRule(inSpace, outSpace, dim, keys)
        (inSpace, outSpace) match {
          case (in: Matrix, out: Vector) => {
            def query = qForwardM2V(in, out, dim, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Vector, out: Matrix) => {
            def query = qBackwardM2V(out, in, dim, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
        }
      }
      case false => {
        //println("flatten mapping forward non-optimization path")
        (inSpace, outSpace) match {
          case (in: Matrix, out: Vector) => qForwardM2V(in, out, dim, keys)
          case (in: Vector, out: Matrix) => qBackwardM2V(out, in, dim, keys)
        }
      }
    }
  }

  def qForwardM2V(in: Matrix, out: Vector, dim: Int, keys: List[Coor]) = {
    require((dim >= 0)&&(dim < 2), {"Input is 2-d, it can only collapse along two dimensions"})
    dim match{
      case 0 => keys.map(key => {
        val k = key.asInstanceOf[Coor2D]
        Coor(in.yDim * k.x + k.y)
      })
      case 1 => keys.map(key => {
        val k = key.asInstanceOf[Coor2D]
        Coor(in.xDim * k.y + k.x)
      })
    }
  }

  def qBackward(keys: List[Coor]) = {
    val flag = keys.map(k => outSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    Mapping.queryOptimization match{
      case true => {
        //println("flatten mapping backward optimization path")
        val rule = FlattenBackwardQueryRule(inSpace, outSpace, dim, keys)
        (inSpace, outSpace) match {
          case (in: Matrix, out: Vector) => {
            def query = qBackwardM2V(in, out, dim, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Vector, out: Matrix) => {
            def query = qForwardM2V(out, in, dim, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
        }
      }
      case false => {
        //println("flatten mapping backward non-optimization path")
        (inSpace, outSpace) match {
          case (in: Matrix, out: Vector) => qBackwardM2V(in, out, dim, keys)
          case (in: Vector, out: Matrix) => qForwardM2V(out, in, dim, keys)
        }
      }
    }
  }

  def qBackwardM2V(in: Matrix, out: Vector, dim: Int, keys: List[Coor]) = {
    require((dim >= 0)&&(dim < 2), {"Input is 2-d, it can only collapse along two dimensions"})
    dim match{
      case 0 => keys.map(key => {
        val k = key.asInstanceOf[Coor1D]
        Coor(k.x/in.yDim, k.x%in.yDim)
      })
      case 1 => keys.map(key => {
        val k = key.asInstanceOf[Coor1D]
        Coor(k.x%in.xDim, k.x/in.xDim)
      })
    }
  }
}

object FlattenMapping{
  /*
   *  Interface from high dimensional spaces to low dimensional spaces
   */ 
  def apply(inMatrix: DenseMatrix[_], outVector: DenseVector[_], dim: Int) = 
    new FlattenMapping(SubSpace(inMatrix), SubSpace(outVector), dim)
  def apply(inVector: DenseVector[_], outMatrix: DenseMatrix[_], dim: Int) = 
    new FlattenMapping(SubSpace(inVector), SubSpace(outMatrix), dim)
  def apply[T](inSV: Seq[DenseVector[T]], outVector: DenseVector[T]) =
    new FlattenMapping(SubSpace(inSV), SubSpace(outVector), 0)
}