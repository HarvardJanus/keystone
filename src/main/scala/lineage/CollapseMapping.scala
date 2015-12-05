package lineage

import breeze.linalg._
import utils.{MultiLabeledImage, Image=>KeystoneImage, ImageMetadata, LabeledImage}

case class CollapseMapping(inSpace: SubSpace, outSpace: SubSpace, dim: Int) extends Mapping{
  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    (inSpace, outSpace) match {
      case (in: Matrix, out: Vector) => qForwardM2V(in, out, dim, keys)
      case (in: Image, out: Matrix) => qForwardI2M(in, out, dim, keys)
      case (in: Vector, out: Matrix) => qBackwardM2V(out, in, dim, keys)
      case (in: Matrix, out: Image) => qBackwardI2M(out, in, dim, keys)
    }
  }

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
    (inSpace, outSpace) match {
      case (in: Matrix, out: Vector) => qBackwardM2V(in, out, dim, keys)
      case (in: Image, out: Matrix) => qBackwardI2M(in, out, dim, keys)
      case (in: Vector, out: Matrix) => qForwardM2V(out, in, dim, keys)
      case (in: Matrix, out: Image) => qForwardI2M(out, in, dim, keys)
    }
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
  def apply(inVector: DenseVector[_], outElement: Int, dim: Int) = 
    new CollapseMapping(SubSpace(inVector), SubSpace(outElement), dim)

  /*
   *  Interface from high dimensional spaces to low dimensional spaces
   */  
  def apply(inMatrix: DenseMatrix[_], outVector: DenseVector[_], dim: Int) = 
    new CollapseMapping(SubSpace(inMatrix), SubSpace(outVector), dim)
  def apply(inImage: KeystoneImage, outMatrix: DenseMatrix[_], dim: Int) = 
    new CollapseMapping(SubSpace(inImage), SubSpace(outMatrix), dim)
  def apply(inImageMeta: ImageMetadata, outMatrix: DenseMatrix[_], dim: Int) = 
    new CollapseMapping(SubSpace(inImageMeta), SubSpace(outMatrix), dim)

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