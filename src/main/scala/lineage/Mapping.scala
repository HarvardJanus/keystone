package lineage

trait Mapping extends Serializable{
  def qForward(keys: List[Coor]): List[Coor]
  def qBackward(keys: List[Coor]): List[Coor]
}