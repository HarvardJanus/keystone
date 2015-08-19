package workflow

import archery._
import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import utils.{MultiLabeledImage, Image, LabeledImage, ImageMetadata}

import java.io._
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.io.Source

trait Mapping extends serializable{
	def qForward(key: Option[_]): List[_]
	def qBackward(key: Option[_]): List[_]
}

case class OneToOneMapping(inRows: Int, inCols: Int, outRows:Int, outCols: Int, 
	seqSize: Int, imageMeta: Option[ImageMetadata] = None) extends Mapping{

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i:Int =>{
				require((inCols == 1)&&(outCols == 1)&&(seqSize == 1), {"input is 2-d structure, use 2-d index"})
				require((i < outRows), {"querying out of boundary of output vector"})
				List(i)
			}
			case (i: Int, j: Int) =>{
				require((seqSize > 1)||(inCols > 1), {"input is 1-d structure, use 1-d index"})
				(outCols, seqSize) match {
					case (1, 1) => {
						//This is the case for matrix-to-vector
						require((j*inRows+i < outRows), {"querying out of boundary of input matrix"})
						List((j*inRows+i))
					}
					case (_, 1) => {
						//This is the case for matrix-to-matrix
						require((i < outRows) && (j < outCols), {"querying out of boundary of input matrix"})
						List((i, j))
					}
					case (1, _) => {
						require((i < seqSize), {"Sequence index out of bound"})
						require((inRows*i+j < outRows), {"querying out of boundary of input vector sequence"})
						List(inRows * i + j)	
					}
				}
			}
			case _ => List()
		}
	}

	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i:Int =>{
        require((outCols == 1), {"output is 2-d structure, use 2-d index"})
				require((i < outRows), {"querying out of boundary of output vector"})
				(inCols, seqSize) match {
  				case (1, 1) => List(i)
  				case (1, _) => List((i/inRows, i%inRows))
  				case (_, 1) => List((i%inRows, i/inRows))
  			}
			}
			case (i: Int, j: Int) =>{
				require((seqSize == 1) && (outCols > 1), {"output is 1-d structure, use 1-d index"})
				require((i < outRows), {"querying out of boundary of output vector"})
				require((j < outCols), {"querying out of boundary of output vector"})
  			List((i, j))
			}
			case _ => List()
		}
	}
}

case class AllToOneMapping(inRows: Int, inCols: Int, outRows: Int, outCols: Int, 
	imageMeta: Option[ImageMetadata] = None) extends Mapping{

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i: Int =>{
				require((inCols == 1), {"input is 2-d structure, use 2-d index"})
				imageMeta match {
					//this is the case where input and output are images
					case Some(meta) => {
						require((i < inRows), {"querying out of boundary of input image"})
						List(i%outRows)
					}
					//this is the case where input and output are 1-d vectors
					case _ => {
						require((i < inRows), {"querying out of boundary of input vector"})
						(0 until outRows).toList
					}
				}
			}
			case (i: Int, j: Int) =>{
				require((inRows > 1)&&(inCols >1), {"input is 1-d structure, use 1-d index"})
				require((i < inRows)&&(j < inCols), {"querying out of boundary of input matrix"})
				val rSeq = for {
					x <- 0 until outRows
					y <- 0 until outCols
				} yield (x, y)
				rSeq.toList
			}
			case _ => List()
		}
	}

	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i: Int =>{
				require((outCols == 1), {"output is 2-d structure, use 2-d index"})
				imageMeta match{
					//this is the case where input and output are images
					case Some(meta) => {
            require((i < outRows), {"querying out of boundary of output image"})
						val xDim = meta.xDim
						val yDim = meta.yDim
						val numChannels = meta.numChannels
						(0 until numChannels).toList.map(c => i+c*xDim*yDim)
					}
					case _ => {
            require((i < inRows), {"querying out of boundray of output vector"})
            (0 until inRows).toList
          }
				}
			}
			case (i: Int, j: Int) => {
				require((inCols > 1), {"input is 1-d structure, use 1-d index"})
				require((i < inRows)&&(j < inCols), {"querying out of boundary of output matrix"})
				val rSeq = for {
					i <- 0 until inRows
					j <- 0 until inCols
				} yield (i, j)
				rSeq.toList
			}
			case _ => List()
		}
	}
}

case class LinComMapping(inRows: Int, inCols: Int, outRows: Int, outCols: Int,
	modelRows: Int, modelCols: Int) extends Mapping{

	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i: Int =>{
				require((outCols == 1)&&(inCols == 1), {"output is 2-d structure, use 2-d index"})
				require((i < outRows), {"querying out of boundary of output vector"})
				List(((0 until inRows).toList, (0 until inRows).toList.zip(List.fill(modelRows){i})))
			}
			case (i: Int, j: Int) => {
				require((outCols > 1)&&(inCols > 1), {"output is 1-d structure, use 1-d index"})
				require((i < outRows)&&(j < outCols), {"querying out of boundary of output matrix"})
				List((List.fill(inCols){i}.zip((0 until inCols).toList), (0 until modelRows).toList.zip(List.fill(modelRows){j})))
			}
		}
	}

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case i: Int =>{
				require((outCols == 1)&&(inCols == 1), {"input is 2-d structure, use 2-d index"})
				require((i < inRows), {"querying out of boundary of input vector"})
				(0 until outRows).toList
			}
			case (i: Int, j: Int) => {
				require((outCols > 1)&&(inCols > 1), {"input is 1-d structure, use 1-d index"})
				require((i < inRows)&&(j < inCols), {"querying out of boundary of input matrix"})
				List.fill(outCols){i}.zip((0 until outCols).toList)
			}
			case _ => List()
		}
	}
}

case class ContourMapping(fMap: Map[Shape, Shape], bMap: Map[Shape, Shape]) extends Mapping{
	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case (i: Int, j: Int) =>{
				val shapeTuple = bMap.find(_._1.inShape(i.toDouble, j.toDouble)).getOrElse(null)
				shapeTuple match {
					case t:(Shape, Shape) => t._2.toCoor
					case _ => List()
				}
			}
		}
	}

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case (i: Int, j: Int) =>{
				val shapeMap= fMap.filter(_._1.inShape(i.toDouble, j.toDouble))
				if(shapeMap.isEmpty){
					List()
				}
				else{
					val shapes = shapeMap.values.toList
					shapes.map(x => x.toCoor)
				}
			}
		}
	}
}

case class ContourMappingRTree(fRTree: RTree[Shape], bRTree: RTree[Shape]) extends Mapping{
  def qBackward(key: Option[_]) = {
    val k = key.getOrElse(null)
    k match {
      case (i: Int, j: Int) =>{
        val shapeArray = bRTree.searchWithIn(Point(i.toFloat, j.toFloat))
        if (shapeArray.isEmpty){
          List()
        }
        else{
          shapeArray.map(x => x.value.toCoor).toList
        }
      }
    }
  }

  def qForward(key: Option[_]) = {
    val k = key.getOrElse(null)
    k match {
      case (i: Int, j: Int) =>{
        val shapeArray = fRTree.searchWithIn(Point(i.toFloat, j.toFloat))
        if(shapeArray.isEmpty){
          List()
        }
        else{
          shapeArray.map(x => x.value.toCoor).toList
        }
      }
    }
  }
}

case class TransposeMapping(inX: Long, inY: Long, outX: Long, outY: Long) extends Mapping{
	//need to check i, j are within bound
	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case (i: Int, j: Int) => {
        require((i < inY)&&(j < inX), {"querying out of bound of input"})
        List((j,i))
      }
			case _ => List()
		}
	}

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case (i: Int, j: Int) => {
        require((i < outY)&&(j < outX), {"querying out of bound of output"})
        List((j,i))
      }
			case _ => List()
		}
	}
}

case class MiscMapping(map: Map[Long, _]) extends Mapping{
  def qForward(key: Option[_]) = {
    val k = key.getOrElse(null)
    k match {
      case (i: Long) =>{
        List(map(i))
      }
    }
  }

  def qBackward(key: Option[_]) = {
    val k = key.getOrElse(null)
    k match {
      case (i: Long) =>{
        List(map(i))
      }
    }
  }
}

object ContourMapping{
	def apply(mapping: List[(List[(Int, Int)], List[(Int, Int)])]) = {
		/*val (fMap, bMap) = buildIndex(mapping)
		new ContourMapping(fMap, bMap)*/
    val (fRTree, bRTree) = buildRTreeIndex(mapping)
    new ContourMappingRTree(fRTree, bRTree)
	}

	def buildIndex(mapping: List[(List[(Int, Int)], List[(Int, Int)])]): (Map[Shape, Shape], Map[Shape, Shape]) = {
		var fMap: Map[Shape, Shape] = Map()
		var bMap: Map[Shape, Shape] = Map()
		val maps = mapping.map{
			m => {
				/*val xList = mapping._1.map(x => x._1)
				val yList = mapping._1.map(x => x._2)
				val x = xList.sum.toDouble/xList.size
				val y = yList.sum.toDouble/yList.size
				val circle = Circle((x, y), 4)

				val upperLeft = (mapping._2.head._1.toDouble, mapping._2.head._2.toDouble)
				val lowerRight = (mapping._2.last._1.toDouble, mapping._2.last._2.toDouble)
				val square = Square(upperLeft, lowerRight)*/
				val circle = Shape(m._1)
				val square = Shape(m._2)

				fMap += circle->square
				bMap += square->circle
			}
		}
		(fMap, bMap)
	}

  def buildRTreeIndex(mapping: List[(List[(Int, Int)], List[(Int, Int)])]): (RTree[Shape], RTree[Shape]) = {
    var fRTree: RTree[Shape] = RTree()
    var bRTree: RTree[Shape] = RTree()
    val maps = mapping.map{
      m => {
        val xList = m._1.map(x => x._1)
        val yList = m._1.map(x => x._2)
        val x = xList.sum.toDouble/xList.size
        val y = yList.sum.toDouble/yList.size
        val circle = Circle((x, y), 4)

        val upperLeft = (m._2.head._1.toDouble, m._2.head._2.toDouble)
        val lowerRight = (m._2.last._1.toDouble, m._2.last._2.toDouble)
        val square = Square(upperLeft, lowerRight)

        fRTree = fRTree.insert(Entry(circle.toBox, square))
        bRTree = bRTree.insert(Entry(square.toBox, circle))
      }
    }
    (fRTree, bRTree)
  }
}

object MiscMapping{
  def apply(mapping: List[(Long, Long)]) = {
    var map: Map[Long, Long] = Map()
    mapping.map(x => map += x._1->x._2)
    new MiscMapping(map)
  }

  def apply(mapping: (Long, Long)) = {
    val map = mapping match {
      case (mIndex:Long, random:Long) => {
        Map(1.toLong->(mIndex, random))
      }
    }
    new MiscMapping(map)
  }
}