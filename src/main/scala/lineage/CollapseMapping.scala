package lineage

import breeze.linalg._
import utils.{MultiLabeledImage, Image=>KeystoneImage, ImageMetadata, LabeledImage}

/*
 *  add RDD[Seq[Vector]] => RDD[Vector]
 */
case class CollapseMapping(inSpace: SubSpace, outSpace: SubSpace, dim: Int) extends Mapping(inSpace, outSpace){
  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    Mapping.queryOptimization match{
      case true => {
        //println("executing the optimization path")
        val rule = CollapseForwardQueryRule(inSpace, outSpace, dim, keys)
        (inSpace, outSpace) match {
          case (in: Vector, out: Singularity) => {
            def query = qForwardV2S(in, out, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Matrix, out: Vector) => {            
            def query = qForwardM2V(in, out, dim, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Image, out: Matrix) => {
            def query = qForwardI2M(in, out, dim, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Singularity, out: Vector) => {
            def query = qBackwardV2S(out, in, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Vector, out: Matrix) => {
            def query = qBackwardM2V(out, in, dim, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Matrix, out: Image) => {
            def query = qBackwardI2M(out, in, dim, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
        }
      }
      case false => {
        //println("executing the unoptimized path")
        (inSpace, outSpace) match {
          case (in: Vector, out: Singularity) => qForwardV2S(in, out, keys)
          case (in: Matrix, out: Vector) => qForwardM2V(in, out, dim, keys)
          case (in: Image, out: Matrix) => qForwardI2M(in, out, dim, keys)
          case (in: Singularity, out: Vector) => qBackwardV2S(out, in, keys)
          case (in: Vector, out: Matrix) => qBackwardM2V(out, in, dim, keys)
          case (in: Matrix, out: Image) => qBackwardI2M(out, in, dim, keys)
        }
      }
    }
  }

  def qForwardV2S(in: Vector, out: Singularity, keys: List[Coor]) = List(Coor(0))

  def qForwardM2V(in: Matrix, out: Vector, dim: Int, keys: List[Coor]) = {
    require((dim >= 0)&&(dim < 2), {"Input is 2-d, it can only collapse along two dimensions"})
    dim match{
      case 0 => keys.map(key => Coor(key.asInstanceOf[Coor2D].y)).distinct
      case 1 => keys.map(key => Coor(key.asInstanceOf[Coor2D].x)).distinct
    }
  }

  def qForwardI2M(in: Image, out: Matrix, dim: Int, keys: List[Coor]) = {
    require((dim >= 0)&&(dim < 3), {"Input is 3-d, it can only collapse along three dimensions"})
    dim match{
      case 0 => keys.map(key => Coor(key.asInstanceOf[Coor3D].y, key.asInstanceOf[Coor3D].c)).distinct
      case 1 => keys.map(key => Coor(key.asInstanceOf[Coor3D].x, key.asInstanceOf[Coor3D].c)).distinct
      case 2 => keys.map(key => Coor(key.asInstanceOf[Coor3D].x, key.asInstanceOf[Coor3D].y)).distinct
    }
  }


  def qBackward(keys: List[Coor]) = {
    val flag = keys.map(k => outSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    Mapping.queryOptimization match{
      case true => {
        //println("executing the optimization path")
        val rule = CollapseBackwardQueryRule(inSpace, outSpace, dim, keys)
        (inSpace, outSpace) match {
          case (in: Vector, out: Singularity) => {
            def query = qBackwardV2S(in, out, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Matrix, out: Vector) => {            
            def query = qBackwardM2V(in, out, dim, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Image, out: Matrix) => {
            def query = qBackwardI2M(in, out, dim, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Singularity, out: Vector) => {
            def query = qForwardV2S(out, in, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Vector, out: Matrix) => {
            def query = qForwardM2V(out, in, dim, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
          case (in: Matrix, out: Image) => {
            def query = qForwardI2M(out, in, dim, _: List[Coor])
            QueryRule.optimizeThenQuery(rule, query)
          }
        }
      }      
      case false => {
        (inSpace, outSpace) match {
          case (in: Vector, out: Singularity) => qBackwardV2S(in, out, keys)
          case (in: Matrix, out: Vector) => qBackwardM2V(in, out, dim, keys)
          case (in: Image, out: Matrix) => qBackwardI2M(in, out, dim, keys)
          case (in: Singularity, out: Vector) => qForwardV2S(out, in, keys)
          case (in: Vector, out: Matrix) => qForwardM2V(out, in, dim, keys)
          case (in: Matrix, out: Image) => qForwardI2M(out, in, dim, keys)
        }
      }
    }
  }

  def qBackwardV2S(in: Vector, out: Singularity, keys: List[Coor]) = {
    in.expand()
  }

  def qBackwardM2V(in: Matrix, out: Vector, dim: Int, keys: List[Coor]) = {
    require((dim >= 0)&&(dim < 2), {"Input is 2-d, it can only collapse along two dimensions"})
    dim match{
      case 0 => keys.flatMap(key => {
        val xDim = in.asInstanceOf[Matrix].xDim
        (0 until xDim).toList.zip(List.fill(xDim){key.asInstanceOf[Coor1D].x}).map{case (x,y)=>Coor(x,y)}
      })
        
      case 1 => keys.flatMap(key => {
        val yDim = in.asInstanceOf[Matrix].yDim
        (0 until yDim).toList.zip(List.fill(yDim){key.asInstanceOf[Coor1D].x}).map{case (x,y)=>Coor(y,x)}
      })
    }
  }

  def qBackwardI2M(in: Image, out: Matrix, dim: Int, keys: List[Coor]) = {
    require((dim >= 0)&&(dim < 3), {"Input is 3-d, it can only collapse along three dimensions"})
    dim match{
      case 0 => keys.flatMap(key => {
        val xDim = in.asInstanceOf[Image].xDim
        (0 until xDim).toList.zip(List.fill(xDim){key.asInstanceOf[Coor2D]}).map{case (x,y)=>Coor(x, y.x, y.y)}
      })
      
      case 1 => keys.flatMap(key => {
        val yDim = in.asInstanceOf[Image].yDim
        (0 until yDim).toList.zip(List.fill(yDim){key.asInstanceOf[Coor2D]}).map{case (x,y)=>Coor(y.x, x, y.y)}
      })

      case 2 => keys.flatMap(key => {
        val cDim = in.asInstanceOf[Image].cDim
        (0 until cDim).toList.zip(List.fill(cDim){key.asInstanceOf[Coor2D]}).map{case (x,y)=>Coor(y.x, y.y, x)}
      })
    }
  }
}

object CollapseMapping{
  def apply(inVector: DenseVector[_], outElement: Int) = 
    new CollapseMapping(SubSpace(inVector), SubSpace(outElement), 0)
  def apply(inElement: Int, outVector: DenseVector[_]) = 
    new CollapseMapping(SubSpace(inElement), SubSpace(outVector), 0)
  /*
   *  Interface from high dimensional spaces to low dimensional spaces
   */  
  def apply(inMatrix: DenseMatrix[_], outVector: DenseVector[_], dim: Int) = 
    new CollapseMapping(SubSpace(inMatrix), SubSpace(outVector), dim)
  def apply(inImage: KeystoneImage, outMatrix: DenseMatrix[_], dim: Int) = 
    new CollapseMapping(SubSpace(inImage), SubSpace(outMatrix), dim)
  def apply(inImageMeta: ImageMetadata, outMatrix: DenseMatrix[_], dim: Int) = 
    new CollapseMapping(SubSpace(inImageMeta), SubSpace(outMatrix), dim)
  def apply(inImage: KeystoneImage, outImage: KeystoneImage, dim: Int) = 
    new CollapseMapping(SubSpace(inImage), SubSpace(outImage), dim)

  /*
   * Interface from low dimensional spaces to high dimensional spaces
   */
  def apply(inVector: DenseVector[_], outMatrix: DenseMatrix[_], dim: Int) = 
    new CollapseMapping(SubSpace(inVector), SubSpace(outMatrix), dim)
  def apply(inMatrix: DenseMatrix[_], outImage: KeystoneImage, dim: Int) = 
    new CollapseMapping(SubSpace(inMatrix), SubSpace(outImage), dim)
  def apply(inMatrix: DenseMatrix[_], outImageMeta: ImageMetadata, dim: Int) = 
    new CollapseMapping(SubSpace(inMatrix), SubSpace(outImageMeta), dim)  

}