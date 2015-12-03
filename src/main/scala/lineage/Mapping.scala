package lineage

import breeze.linalg._
import utils.{MultiLabeledImage, Image=>KeystoneImage, ImageMetadata, LabeledImage}

trait Mapping extends Serializable{
  def qForward(keys: List[Coor]): List[Coor]
  def qBackward(keys: List[Coor]): List[Coor]
}

case class IdentityMapping(inSpace: SubSpace, outSpace: SubSpace) extends Mapping{
  def qForward(keys: List[Coor]) = {
    val flag = keys.map(k => outSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    keys
  }

  def qBackward(keys: List[Coor]) = {
    val flag = keys.map(k => inSpace.contain(k)).reduce(_ && _)
    require((flag==true), {"query out of subspace boundary"})
    keys
  }
}

object IdentityMapping{
  def apply(inVector: DenseVector[_], outVector: DenseVector[_]) = new IdentityMapping(SubSpace(inVector), SubSpace(outVector))
  def apply(inMatrix: DenseMatrix[_], outMatrix: DenseMatrix[_]) = new IdentityMapping(SubSpace(inMatrix), SubSpace(outMatrix))
  def apply(inImage: KeystoneImage, outImage: KeystoneImage) = new IdentityMapping(SubSpace(inImage), SubSpace(outImage))
  def apply(inImageMeta: ImageMetadata, outImageMeta: ImageMetadata) = new IdentityMapping(SubSpace(inImageMeta), SubSpace(outImageMeta))
}
