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
		val secondItem = ((j-y)*cos(theta) - (i-x)*sin(theta))/b
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
			i <- (x-b).toInt to (x+b).toInt
			j <- (y-a).toInt to (y+a).toInt
		} yield (i, j)
		l.toList
	}

	def inShape(i: Double, j: Double): Boolean = {
		val x = c._1
		val y = c._2
		if(i>=(x-b) && i<=(x+b) && j>=(y-a) && j<=(y+a)) true else false
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

object Shape{
	def apply(input: List[(Int, Int)]): Shape = {
		detect(input)
	}

	def euclideanDistance(x: (Double, Double), y: (Double, Double)) = {
		(x._1-y._1)*(x._1-y._1) + (x._2-y._2)*(x._2-y._2)
	}

	def detect(input: List[(Int, Int)]): Shape = {
		val x = input.map(x => x._1.toDouble)
		val y = input.map(x => x._2.toDouble)
		val xCentroid = x.sum/x.length
		val yCentroid = y.sum/y.length
		val centroid = (xCentroid, yCentroid)

		//fit a square
		val xLeft = x.min
		val xRight = x.max
		val yUp = y.min
		val yDown = y.max
		val square = Square((xLeft, yUp), (xRight, yDown))

		val furthest = {
			var distance = 0.0
			var point = (0.0, 0.0)
			for(p <- x.zip(y)){
				val dist = euclideanDistance(p, centroid)
				if ( dist > distance){
					distance = dist
					point = p
				}
			}
			point
		}

		//fit a circle
		val r = sqrt(euclideanDistance(furthest, centroid))
		val circle = Circle(centroid, r)

		//fit an ellipse
		val theta = if(((furthest._1 <= centroid._1)&&(furthest._2 <= centroid._2))||((furthest._1 > centroid._1)&&(furthest._2 > centroid._2))) asin(-centroid._1/r) else asin (centroid._1/r)
		val numerator = pow(r*((furthest._2-centroid._2)*cos(theta)-(furthest._1-centroid._1)*sin(theta)), 2)
		val denominator = pow(r, 2)-pow(((furthest._1-centroid._1)*cos(theta)+(furthest._2-centroid._2)*sin(theta)), 2)
		val b = sqrt(numerator/denominator)
		val ellipse = Ellipse(centroid, r, b, theta)

		//calculate the precision of each shape
		val pre_square = if (square.toCoor.size == 0) 0 else square.toCoor.intersect(input).size.toDouble/square.toCoor.size
		val pre_circle = if (circle.toCoor.size == 0) 0 else circle.toCoor.intersect(input).size.toDouble/circle.toCoor.size
		val pre_ellipse = if (ellipse.toCoor.size == 0) 0 else ellipse.toCoor.intersect(input).size.toDouble/ellipse.toCoor.size

		//println("square: "+pre_square+"\tcircle: "+pre_circle+"\tellipse: "+pre_ellipse)

		//decide shape based on the accuracy of the fitting shape
		val shape = if ((pre_ellipse >= pre_circle)&&(pre_ellipse >= pre_square)){
			ellipse
		}else if (pre_circle >= pre_square) {
			circle
		}else{
			square
		}

		return shape
	}
}
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

		val list = List((0, 0), (0, 1), (1, 0), (1, 1))
		val shape = Shape(list)
		println(shape.toCoor)
  }
}