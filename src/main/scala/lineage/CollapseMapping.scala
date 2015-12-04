package lineage

import breeze.linalg._
import utils.{MultiLabeledImage, Image=>KeystoneImage, ImageMetadata, LabeledImage}

case class CollapseMapping(inSpace: SubSpace, outSpace: SubSpace, dim: Int) extends Mapping{
  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    outSpace.expand()
  }

  def qBackward(keys: List[Coor]) = {
    val flag = keys.map(k => outSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    outSpace.expand()
  }
}

object CollapseMapping{
  def apply(inVector: DenseVector[_], outElement: Int, dim: Int) = 
    new CollapseMapping(SubSpace(inVector), SubSpace(outElement), dim)
  def apply(inMatrix: DenseMatrix[_], outVector: DenseVector[_], dim: Int) = 
    new CollapseMapping(SubSpace(inMatrix), SubSpace(outVector), dim)
  def apply(inImage: KeystoneImage, outMatrix: DenseMatrix[_], dim: Int) = 
    new CollapseMapping(SubSpace(inImage), SubSpace(outMatrix), dim)
  def apply(inImageMeta: ImageMetadata, outMatrix: DenseMatrix[_], dim: Int) = 
    new CollapseMapping(SubSpace(inImageMeta), SubSpace(outMatrix), dim)
}