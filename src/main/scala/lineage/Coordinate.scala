package lineage

abstract class Coor extends Serializable{
  def first(): Int
  def lower(): Coor
  def raise(i: Int): Coor
}


case class Coor1D(x: Int) extends Coor {
  require(x>=0, {"coordinate can not be negative"})
  override def toString = x.toString
  def first() = x
  def lower() = Coor()
  def raise(i: Int) = Coor(i, x)
}

case class Coor2D(x:Int, y:Int) extends Coor {
  require((x>=0)&&(y>=0), {"coordinate can not be negative"})
  override def toString = "("+x+","+y+")"
  def first() = x
  def lower() = Coor(y)
  def raise(i: Int) = Coor(i, x, y)
}

case class Coor3D(x:Int, y:Int, c:Int) extends Coor {
  require((x>=0)&&(y>=0)&&(c>=0), {"coordinate can not be negative"})
  override def toString = "("+x+","+y+","+c+")"
  def first() = x
  def lower() = Coor(y, c)
  def raise(i: Int) = Coor(i, x, y, c)
}

case class Coor4D(x:Int, y:Int, c:Int, d: Int) extends Coor {
  require((x>=0)&&(y>=0)&&(c>=0)&&(d>=0), {"coordinate can not be negative"})
  override def toString = "("+x+","+y+","+c+","+d+")"
  def first() = x
  def lower() = Coor(y, c, d)
  /*
   *  The following raise method is a place holder.
   */
  def raise(i: Int) = Coor(x, y, c, d)
}

case class CoorNull() extends Coor{
  override def toString = "null"
  def first() = 0
  def lower() = this
  def raise(i: Int) = Coor(i)
}

object Coor{
  def apply(x:Int): Coor = new Coor1D(x)
  def apply(x:Int, y:Int): Coor = new Coor2D(x, y)
  def apply(x:Int, y:Int, c:Int): Coor = new Coor3D(x, y, c)
  def apply(x:Int, y:Int, c:Int, d:Int): Coor = new Coor4D(x, y, c, d)
  def apply() = new CoorNull()
}