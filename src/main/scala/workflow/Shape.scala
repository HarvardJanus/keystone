package workflow

import scala.math._

abstract class Shape(c: (Double, Double)) extends Serializable{
	def getCenter(): (Double, Double)
	def toCoor(): List[_]
	def inShape(i: Double, j: Double): Boolean
}

class Circle(c: (Double, Double), r: Double) extends Shape(c){
	def toCoor() = {
		val x = c._1
		val y = c._2
		val l = for { 
			i <- (x-r).toInt to (x+r).toInt
			j <- (y-r).toInt to (y+r).toInt
			if (i-x)*(i-x)+(j-y)*(j-y) <= r*r
		} yield (i, j)
		l.toList
	}

	def inShape(i: Double, j: Double): Boolean = {
		val x = c._1
		val y = c._2
		if ((i-x)*(i-x)+(j-y)*(j-y) <= r*r) true else false
	}

	override def toString() = "center: "+c+" r: "+r
	def getCenter() = c
	def getR() = r
}

case class Ellipse(c: (Double, Double), a: Double, b: Double, theta: Double) extends Shape(c){
	def getCenter() = c
	def toCoor() = {
		val x = c._1
		val y = c._2
		val l = for { 
			i <- (x-a).toInt to (x+a).toInt
			j <- (y-a).toInt to (y+a).toInt
			if inShape(i, j)
		} yield (i, j)
		l.toList
	}

	def inShape(i: Double, j: Double): Boolean = {
		val x = c._1
		val y = c._2
		val firstItem = ((i-x)*cos(theta) + (j-y)*sin(theta))/a
		val secondItem = ((i-x)*sin(theta) + (j-y)*cos(theta))/b
		if(firstItem*firstItem + secondItem*secondItem <= 1) true else false
	}

	override def toString() = "center: "+c+" major: "+a+" minor: "+b+" theta: "+theta
}

case class Square(c: (Double, Double), a: Double, b:Double) extends Shape(c){
	def getCenter() = c
	def toCoor() = {
		val x = c._1
		val y = c._2
		val l = for { 
			i <- (x-a).toInt to (x+a).toInt
			j <- (y-b).toInt to (y+b).toInt
		} yield (i, j)
		l.toList
	}

	def inShape(i: Double, j: Double): Boolean = {
		val x = c._1
		val y = c._2
		if(i>=(x-a) && i<=(x+a) && j>=(y-b) && j<=(y+b)) true else false
	}

	override def toString() = "center: "+c+" width: "+2*a+" height: "+2*b
}

object Circle{
	def apply(c: (Double, Double), r: Double): Shape = {
		new Circle(c, r)
	}
}

/*object Ellipse{
	override def apply(c: (Double, Double), a: Double, b: Double, theta: Double): Shape = {
		require((a >= b), {"ellipse major has to be greater than or equal to minor"})
		new Ellipse(c, a, b, theta)
	}
}*/

object Square{
	def apply(upperLeft: (Double, Double), lowerRight: (Double, Double)): Shape = {
		val c = ((upperLeft._1 + lowerRight._1)/2, (upperLeft._2 + lowerRight._2)/2)
		val a = abs(upperLeft._2 - lowerRight._2)/2
		val b = abs(upperLeft._1 - lowerRight._1)/2
		new Square(c, a, b)
	}
}
object ShapeTester{
	def main(args: Array[String]) {
    val circle = Circle((0, 0), 2.0)
    println(circle.toCoor)

    val ellipse = Ellipse((0, 0), 2, 1, 0)
    println(ellipse.toCoor)

    val square = Square((0, 0), 1, 1)
    println(square.toCoor)

    val square1 = Square((-1, -1), (1, 1))
		println(square1.toCoor)
  }
}