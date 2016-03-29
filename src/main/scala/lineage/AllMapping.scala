package lineage

import breeze.linalg._
import utils.{MultiLabeledImage, Image=>KeystoneImage, ImageMetadata, LabeledImage}

case class AllMapping(inSpace: SubSpace, outSpace: SubSpace) extends Mapping(inSpace, outSpace){
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

object AllMapping{
  def apply(inVector: DenseVector[_], outVector: DenseVector[_]) = new AllMapping(SubSpace(inVector), SubSpace(outVector))
  def apply(inMatrix: DenseMatrix[_], outMatrix: DenseMatrix[_]) = new AllMapping(SubSpace(inMatrix), SubSpace(outMatrix))
  def apply(inImage: KeystoneImage, outImage: KeystoneImage) = new AllMapping(SubSpace(inImage), SubSpace(outImage))
  def apply(inImageMeta: ImageMetadata, outImageMeta: ImageMetadata) = new AllMapping(SubSpace(inImageMeta), SubSpace(outImageMeta))
}