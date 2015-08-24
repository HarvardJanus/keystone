package workflow

import archery._
import scala.math._

abstract class Shape(c: (Double, Double)) extends Serializable{
	def getCenter(): (Double, Double)
	def toCoor(): List[_]
	def inShape(i: Double, j: Double): Boolean
	def toBox(): Box
	def toSquare(): Square
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

	def toBox() = Box((c._1-r).toFloat, (c._2-r).toFloat, (c._1+r).toFloat, (c._2+r).toFloat)
	def toSquare = Square(((c._1-r).toDouble, (c._2-r).toDouble), ((c._1+r).toDouble, (c._2+r).toDouble)).asInstanceOf[Square]

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

	def toBox() = {
		/**
		 *  In an ellipse with rotation
		 *  x = c._1 + a*cos(t)*cos(theta) - b*sin(t)*sin(theta)
		 *	y = c._2 + b*sin(t)*cos(theta) + a*cos(t)*sin(phi)
		 *
		 *	dx/dt = -a*sin(t)*cos(theta) - b*cos(t)*sin(theta) = 0
		 *	dy/dt = b*cos(t)*cos(theta) - a*sin(t)*sin(theta) = 0
		 */
		val xTan = -b*tan(theta)/a
		val t1 = atan(xTan)
		val t2 = t1+Pi
		val x1 = c._1 + a*cos(t1)*cos(theta) - b*sin(t1)*sin(theta)
		val x2 = c._1 + a*cos(t2)*cos(theta) - b*sin(t2)*sin(theta)
		val xl = List(x1, x2)
		val xMin = xl.min
		val xMax = xl.max

		val yTan = b*cos(theta)/sin(theta)/a
		val t3 = atan(yTan)
		val t4 = t3+Pi
		val y1 = c._2 + b*sin(t3)*cos(theta) + a*cos(t3)*sin(theta)
		val y2 = c._2 + b*sin(t4)*cos(theta) + a*cos(t4)*sin(theta)
		val yl = List(y1, y2)
		val yMin = yl.min
		val yMax = yl.max
		Box(xMin.toFloat, yMin.toFloat, xMax.toFloat, yMax.toFloat)
	}

	def toSquare() ={
		val xTan = -b*tan(theta)/a
		val t1 = atan(xTan)
		val t2 = t1+Pi
		val x1 = c._1 + a*cos(t1)*cos(theta) - b*sin(t1)*sin(theta)
		val x2 = c._1 + a*cos(t2)*cos(theta) - b*sin(t2)*sin(theta)
		val xl = List(x1, x2)
		val xMin = xl.min
		val xMax = xl.max

		val yTan = b*cos(theta)/sin(theta)/a
		val t3 = atan(yTan)
		val t4 = t3+Pi
		val y1 = c._2 + b*sin(t3)*cos(theta) + a*cos(t3)*sin(theta)
		val y2 = c._2 + b*sin(t4)*cos(theta) + a*cos(t4)*sin(theta)
		val yl = List(y1, y2)
		val yMin = yl.min
		val yMax = yl.max
		Square((xMin, yMin), (xMax, yMax)).asInstanceOf[Square]
	}

	override def toString() = "center: "+c+" major: "+a+" minor: "+b+" theta: "+theta
}

case class Square(c: (Double, Double), a: Double, b:Double) extends Shape(c){
	def getCenter() = c
	def getUpperLeft = (c._1-b, c._2-a)
	def getLowerRight = (c._1+b, c._2+a)
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

	def toBox() = Box((c._1-b).toFloat, (c._2-a).toFloat, (c._1+b).toFloat, (c._2+a).toFloat)
	def toSquare() = Square(((c._1-b), (c._2-a)), ((c._1+b), (c._2+a))).asInstanceOf[Square]

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
		sqrt((x._1-y._1)*(x._1-y._1) + (x._2-y._2)*(x._2-y._2))
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
		val r = euclideanDistance(furthest, centroid)
		val circle = Circle(centroid, r)

		//fit an ellipse
		val theta = if(((furthest._1 <= centroid._1)&&(furthest._2 <= centroid._2))||((furthest._1 > centroid._1)&&(furthest._2 > centroid._2))) asin(-(furthest._2-centroid._2)/r) else asin ((furthest._2-centroid._2)/r)

		val secondList = x.zip(y).filter{
			x => (x != furthest) && (abs(abs(asin((x._2-centroid._2)/euclideanDistance(x, centroid)) - theta) - 1.57) < 0.01)
		}
		val second = secondList.sortWith(euclideanDistance(_,centroid)>euclideanDistance(_,centroid)).head

		val numerator = pow(r*((second._2-centroid._2)*cos(theta)-(second._1-centroid._1)*sin(theta)), 2)
		val denominator = pow(r, 2)-pow(((second._1-centroid._1)*cos(theta)+(second._2-centroid._2)*sin(theta)), 2)

		val b = sqrt(numerator/denominator)
		val ellipse = Ellipse(centroid, r, b, theta)

		//calculate the precision of each shape
		val pre_square = if (square.toCoor.size == 0 || square.toCoor.size < input.size) 0 else square.toCoor.intersect(input).size.toDouble/square.toCoor.size
		val pre_circle = if (circle.toCoor.size == 0 || circle.toCoor.size < input.size) 0 else circle.toCoor.intersect(input).size.toDouble/circle.toCoor.size
		val pre_ellipse = if (ellipse.toCoor.size == 0 || ellipse.toCoor.size < input.size) 0 else ellipse.toCoor.intersect(input).size.toDouble/ellipse.toCoor.size

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