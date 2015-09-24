package workflow

import archery._
import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import utils.{MultiLabeledImage, Image, LabeledImage, ImageMetadata}

import java.io._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.math._
import scala.reflect.ClassTag
import scala.io.Source

trait Mapping extends serializable{
	def qForward(key: Option[_]): List[_]
	def qBackward(key: Option[_]): List[_]
}

object Mapping{
  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    (System.nanoTime-s)/1e6
  }

  def querySeq(m: Mapping, xDim: Int, yDim: Int) = {
    val timeVector = for(i <- (0 until xDim); j<- (0 until yDim))
      yield(time(m.qBackward(Some(i,j))))
    timeVector.toList
  }
}

case class ElementMapping(inMeta: Metadata, outMeta: Metadata) extends Mapping{
  def query(key: Option[_], inMeta: Metadata, outMeta: Metadata) = {
    (inMeta, outMeta) match {
      case (in: VectorMeta, out: VectorMeta) => {
        val k = key.getOrElse(null)
        k match {
          case i:Int => {
            require((i < in.dim)&&(i < out.dim), {"querying out of boundary of input vector"})
            List(i)
          }
          case _ => {
            require(0==1, {"input is 1-d structure, use 1-d index"})
            List()
          }
        }
      }
      case (in: VectorMeta, out: MatrixMeta) => {
        val k = key.getOrElse(null)
        k match {
          case i:Int => {
            require((i < in.dim), {"querying out of boundary of input vector"})
            require((i < out.xDim*out.yDim), {"querying out of boundary of output matrix"})
            List((i%out.xDim, i/out.xDim))
          }
          case _ => {
            require(0==1, {"input is 1-d structure, use 1-d index"})
            List()
          }
        }
      }
      case (in: MatrixMeta, out: MatrixMeta) => {
        val k = key.getOrElse(null)
        k match {
          case (i:Int, j:Int) => {
            require((i < in.xDim)&&(j < in.yDim), {"querying out of boundary of input matrix"})
            require((i < out.xDim)&&(j < out.yDim), {"querying out of boundary of output matrix"})
            List((i,j))
          }
          case _ => {
            require(0==1, {"input is 2-d structure, use 2-d index"})
            List()
          }
        }
      }
      case (in: MatrixMeta, out: VectorMeta) => {
        val k = key.getOrElse(null)
        k match {
          case (i:Int, j:Int) => {
            require((i < in.xDim)&&(j < in.yDim), {"querying out of boundary of input matrix"})
            require(j*in.xDim+i < out.dim, {"querying out of boundary of output vector"})
            List(j*in.xDim+i)
          }
          case _ => {
            require(0==1, {"input is 2-d structure, use 2-d index"})
            List()
          }
        }
      }
      case (in: ImageMeta, out: ImageMeta) => {
        val k = key.getOrElse(null)
        k match {
          case (i:Int, j:Int) => {
            require((in.cDim==1), "input image has multiple channels, use 3-d index")
            require((i < in.xDim)&&(j < in.yDim), {"querying out of boundary of input image"})
            require((i < out.xDim)&&(j < out.yDim), {"querying out of boundary of output image"})
            List((i,j,0))
          }
          case (i:Int, j:Int, c:Int) => {
            require((i < in.xDim)&&(j < in.yDim)&&(c < in.cDim), {"querying out of boundary of input image"})
            require((i < out.xDim)&&(j < out.yDim)&&(c < out.cDim), {"querying out of boundary of output image"})
            List((i,j,c))
          }
          case _ => {
            require(0==1, {"input is 3-d structure, use 3-d index"})
            List()
          }
        }
      }
    }
  }

  def qForward(key: Option[_]) = query(key, inMeta, outMeta)
  def qBackward(key: Option[_]) = query(key, outMeta, inMeta)
}

case class AllMapping(inMeta: Metadata, outMeta: Metadata) extends Mapping{
  def query(key: Option[_], inMeta: Metadata, outMeta: Metadata) = {
    (inMeta, outMeta) match {
      case (in: VectorMeta, out: VectorMeta) => {
        val k = key.getOrElse(null)
        k match {
          case i:Int => {
            require((i < in.dim), {"querying out of boundary of input vector"})
            (0 until out.dim).toList
          }
          case _ => {
            require(0==1, {"input is 1-d structure, use 1-d index"})
            List()
          }
        }
      }
      case (in: MatrixMeta, out: MatrixMeta) => {
        val k = key.getOrElse(null)
        k match {
          case (i:Int, j:Int) => {
            require((i < in.xDim)&&(j < in.yDim), {"querying out of boundary of input matrix"})
            require((i < out.xDim)&&(j < out.yDim), {"querying out of boundary of output matrix"})
            val rSeq = for {
              x <- 0 until out.xDim
              y <- 0 until out.yDim
            } yield (x, y)
            rSeq.toList
          }
          case _ => {
            require(0==1, {"input is 2-d structure, use 2-d index"})
            List()
          }
        }
      }
      case (in: ImageMeta, out: ImageMeta) => {
        val k = key.getOrElse(null)
        k match {
          case (i:Int, j:Int) => {
            require((in.cDim==1), "input image has multiple channels, use 3-d index")
            require((i < in.xDim)&&(j < in.yDim), {"querying out of boundary of input image"})
            require((i < out.xDim)&&(j < out.yDim), {"querying out of boundary of output image"})
            List.fill(out.cDim){(i,j)}.zip((0 until out.yDim)).map{case (t, c) => (t._1, t._2, c)}
          }
          case (i:Int, j:Int, c:Int) => {
            require((i < in.xDim)&&(j < in.yDim)&&(c < in.cDim), {"querying out of boundary of input image"})
            require((i < out.xDim)&&(j < out.yDim)&&(c < out.cDim), {"querying out of boundary of output image"})
            out.cDim match {
              case 1 => {
                List((i,j,0))
              }
              case _ =>{
                val rSeq = for {
                  x <- 0 until out.xDim
                  y <- 0 until out.yDim
                  c <- 0 until out.cDim
                } yield (x, y, c)
                rSeq.toList
              }
            }
            
          }
          case _ => {
            require(0==1, {"input is 3-d structure, use 3-d index"})
            List()
          }
        }
      }
    }
  }

  def qForward(key: Option[_]) = query(key, inMeta, outMeta)
  def qBackward(key: Option[_]) = query(key, outMeta, inMeta)
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

case class ContourMapping(fMap: Map[_<:Shape, _<:Shape], bMap: Map[_<:Shape, _<:Shape]) extends Mapping{
	def query(key: Option[_], map: Map[_<:Shape, _<:Shape]) = {
		val k = key.getOrElse(null)
		k match {
			case (i: Int, j: Int) =>{
				val shapeMap= map.filter(_._1.inShape(i.toDouble, j.toDouble))
				if(shapeMap.isEmpty){
					List()
				}
				else{
					val shapes = shapeMap.values.toList
					shapes.map(x => x.toCoor)
				}
			}
      case _ => {
        require((0==1), "input is 2-d structure, use 2-d index")
        List()
      }
		}
	}

  def qForward(key: Option[_]) = query(key, fMap)
  def qBackward(key: Option[_]) = query(key, bMap) 
}

case class ContourMappingDirect(fIndex: Map[(Int, Int), List[Shape]], bIndex: Map[(Int, Int), List[Shape]], 
  fMap: Map[Shape, Shape], bMap: Map[Shape, Shape]) extends Mapping{

  def query(key: Option[_], index: Map[(Int, Int), List[Shape]], map: Map[Shape, Shape]) = {
    val k = key.getOrElse(null)
    k match {
      case (i: Int, j: Int) =>{
        val shapeList = index.getOrElse((i,j), List())
        shapeList.map(s => map(s).toCoor)
      }
      case _ => {
        require((0==1), "input is 2-d structure, use 2-d index")
        List()
      }
    }
  }

  def qForward(key: Option[_]) = query(key, fIndex, fMap)
  def qBackward(key: Option[_]) = query(key, bIndex, bMap)  
}

case class ContourMappingRTree(fRTree: RTree[Shape], bRTree: RTree[Shape],
  fMap: Map[Shape, Shape], bMap: Map[Shape, Shape]) extends Mapping{

  def query(key: Option[_], rTree: RTree[Shape], map: Map[Shape, Shape]) = {
    val k = key.getOrElse(null)
    k match {
      case (i: Int, j: Int) =>{
        val shapeArray = rTree.searchWithIn(Point(i.toFloat, j.toFloat))
        if (shapeArray.isEmpty){
          List()
        }
        else{
          shapeArray.filter(x=>x.value.inShape(i.toDouble, j.toDouble)).map(x => map(x.value).toCoor).toList
        }
      }
      case _ => {
        require((0==1), "input is 2-d structure, use 2-d index")
        List()
      }
    }
  }

  def qForward(key: Option[_]) = query(key, fRTree, fMap)
  def qBackward(key: Option[_]) = query(key, bRTree, bMap)  
}

case class ContourMappingKMeans(fIndex: Map[Shape, List[Shape]], bIndex: Map[Shape, List[Shape]],
  fMap: Map[Shape, Shape], bMap: Map[Shape, Shape]) extends Mapping{
  def query(key: Option[_], index: Map[Shape, List[Shape]], map: Map[Shape, Shape]) = {
    val k = key.getOrElse(null)
    k match {
      case (i: Int, j: Int) =>{
        val keyList = index.keys.toList.filter(_.inShape(i.toDouble, j.toDouble))
        if (keyList.isEmpty){
          List()
        }
        else{
          val shapeList = keyList.flatMap(x => index(x).filter(s => s.inShape(i.toDouble, j.toDouble)))
          shapeList.map(x=>map(x).toCoor)
        }
      }
      case _ => {
        require((0==1), "input is 2-d structure, use 2-d index")
        List()
      }
    }
  }

  def qForward(key: Option[_]) = query(key, fIndex, fMap)
  def qBackward(key: Option[_]) = query(key, bIndex, bMap)
}

case class SimpleMapping(fIndex: Map[(Int, Int), List[List[(Int, Int)]]], bIndex: Map[(Int, Int), List[List[(Int, Int)]]]) extends Mapping{
  def qForward(key: Option[_]) = query(key, fIndex)
  def qBackward(key: Option[_]) = query(key, bIndex)

  def query(key: Option[_], index: Map[(Int, Int), List[List[(Int, Int)]]]) = {
    val k = key.getOrElse(null)
    k match {
      case (i: Int, j: Int) =>{
        val valueList = index((i, j))
        if (valueList.isEmpty){
          List()
        }
        else{
          valueList
        }
      }
      case _ => {
        require((0==1), "input is 2-d structure, use 2-d index")
        List()
      }
    }
  }
}

case class OneManyMapping(fIndex: (Map[(Int, Int), List[String]], Map[String,List[(Int, Int)]]), 
    bIndex: (Map[(Int, Int), List[String]], Map[String,List[(Int, Int)]])) extends Mapping{
  def qForward(key: Option[_]) = query(key, fIndex)
  def qBackward(key: Option[_]) = query(key, bIndex)

  def query(key: Option[_], index: (Map[(Int, Int), List[String]], Map[String,List[(Int, Int)]])) = {
    val firstIndex = index._1
    val secondIndex = index._2
    val k = key.getOrElse(null)
    k match {
      case (i: Int, j: Int) =>{
        val valueList = firstIndex((i, j))
        if (valueList.isEmpty){
          List()
        }
        else{
          valueList.map(key => secondIndex(key))
        }
      }
      case _ => {
        require((0==1), "input is 2-d structure, use 2-d index")
        List()
      }
    }
  }
}

case class TransposeMapping(inX: Long, inY: Long, outX: Long, outY: Long) extends Mapping{
  require((inX == outY)&&(inY == outX), {"dimensions of input and output matrix are not matching"})

	def qBackward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case (i: Int, j: Int) => {
        require((i < inY)&&(j < inX), {"querying out of bound of input"})
        List((j,i))
      }
			case _ => {
        require((0==1), "input is 2-d structure, use 2-d index")
        List()
      }
		}
	}

	def qForward(key: Option[_]) = {
		val k = key.getOrElse(null)
		k match {
			case (i: Int, j: Int) => {
        require((i < outY)&&(j < outX), {"querying out of bound of output"})
        List((j,i))
      }
			case _ => {
        require((0==1), "input is 2-d structure, use 2-d index")
        List()
      }
		}
	}
}

object ContourMapping{

  def apply(mapping: List[(Shape, Shape)]) = {
    val (fMap, bMap) = buildIndex(mapping)
    new ContourMapping(fMap, bMap)
    /*val (fIndex, bIndex, fMap, bMap) = buildDirectIndex(mapping)
    new ContourMappingDirect(fIndex, bIndex, fMap, bMap)*/
    /*val (fRTree, bRTree, fMap, bMap) = buildRTreeIndex(mapping)
    new ContourMappingRTree(fRTree, bRTree, fMap, bMap)*/
    /*val (fIndex, bIndex, fMap, bMap) = buildKMeansIndex(mapping)
    new ContourMappingKMeans(fIndex, bIndex, fMap, bMap)*/
  }

  def buildIndex(mapping: List[(Shape, Shape)]): (Map[Shape, Shape], Map[Shape, Shape]) = {
    var fMap: Map[Shape, Shape] = Map()
    var bMap: Map[Shape, Shape] = Map()
    mapping.map( 
      x => {
        fMap += x._1->x._2
        bMap += x._2->x._1
      })
    (fMap, bMap)
  }

  def buildDirectIndex(mapping: List[(Shape, Shape)]): 
      (Map[(Int, Int), List[Shape]], Map[(Int, Int), List[Shape]], Map[Shape, Shape], Map[Shape, Shape]) = {

    var fMap: Map[Shape, Shape] = Map()
    var bMap: Map[Shape, Shape] = Map()
    var fIndex: Map[(Int, Int), List[Shape]] = Map()
    var bIndex: Map[(Int, Int), List[Shape]] = Map()

    /*need to change to automatic shape detection*/
    val maps = mapping.map{
      m => {
        /*add entries to fMap and bMap*/
        fMap += m._1->m._2
        bMap += m._2->m._1

        /*add entries to fIndex[(Int, Int), List(Shape)]*/
        m._1.toCoor.map(t => {
          if(fIndex.contains(t)){
            fIndex(t) = fIndex(t) :+ m._1
          }
          else{
            fIndex += t->List(m._1)
          }
        })
        /*add entries to bIndex[(Int, Int), List(Shape)]*/
        m._2.toCoor.map(t => {
          if(bIndex.contains(t)){
            bIndex(t) = bIndex(t) :+ m._2
          }
          else{
            bIndex += t->List(m._2)
          }
        })
      }
    }
    (fIndex, bIndex, fMap, bMap)
  }

  def buildRTreeIndex(mapping: List[(Shape, Shape)]): 
      (RTree[Shape], RTree[Shape], Map[Shape, Shape], Map[Shape, Shape]) = {
    var fMap: Map[Shape, Shape] = Map()
    var bMap: Map[Shape, Shape] = Map()
    var fRTree: RTree[Shape] = RTree()
    var bRTree: RTree[Shape] = RTree()

    /*need to change to automatic shape detection*/
    val maps = mapping.map{
      m => {
        fMap += m._1->m._2
        bMap += m._2->m._1
        fRTree = fRTree.insert(Entry(m._1.toBox, m._1))
        bRTree = bRTree.insert(Entry(m._2.toBox, m._2))
      }
    }
    (fRTree, bRTree, fMap, bMap)
  }

  def buildKMeansIndex(mapping: List[(Shape, Shape)]): 
      (Map[Shape, List[Shape]], Map[Shape, List[Shape]], Map[Shape, Shape], Map[Shape, Shape]) = {
    var fMap: Map[Shape, Shape] = Map()
    var bMap: Map[Shape, Shape] = Map()
    var fIndex: Map[Shape, List[Shape]] = Map()
    var bIndex: Map[Shape, List[Shape]] = Map()

    /* preprocessing, converting mapping to List[(Shape, Shape)]
     * need to change to automatic shape detection
     */
    val shapeMap = mapping.map{
      m => {
        fMap += m._1->m._2
        bMap += m._2->m._1
      }
    }
    val fShapeList = mapping.map(x => x._1)
    val bShapeList = mapping.map(x => x._2)

    /*definition of KMeans with iteration as parameter*/
    def KMeans(mapping: List[Shape], iteration: Int): Map[Shape, List[Shape]] = {
      val listSize = mapping.size
      val numBuckets = sqrt(listSize).toInt
      val partitions = mapping.sliding(numBuckets, numBuckets)

      /*initialize a new map with centroid of the list as key*/
      var centroidMap = partitions.zipWithIndex.map{
        case (l, index) => (index.toDouble, index.toDouble)->l.to[ListBuffer]
      }.toMap

      /*KMeans iteration*/
      (0 until iteration).map{
        i =>{
          val newCentroidMap = centroidMap.map{
            case (key, l) => getCentroid(l.toList)->ListBuffer[Shape]()
          }
          mapping.map{
            t =>{
              val closest = newCentroidMap.keys.toList.minBy(x => Shape.euclideanDistance(x, t.getCenter))
              newCentroidMap(closest) += t
            }
          }
          centroidMap = newCentroidMap
        }
      }

      /*convert Map[centroid->List[(Shape, Shape)]] to Map[Shape->List[(Shape, Shape)]]*/
      val seq = centroidMap.map{
        case (key, l) => getBoundSquare(key, l.toList)->l.toList
      }.toSeq

      Map(seq: _*)
    }

    /*helper function computes the centroid of a given list of shapes*/
    def getCentroid(l: List[Shape]): (Double, Double) = {
      val xSum = l.map(x => x.getCenter._1.toDouble).sum
      val ySum = l.map(x => x.getCenter._2.toDouble).sum
      (xSum/l.size, ySum/l.size)
    }

    /*helper function computes the bounding square of a given list of shapes*/
    def getBoundSquare(key: (Double, Double), l: List[Shape]): Shape = {
      l.foldLeft(Square(key, 0.0, 0.0)){
        (x, y) => {
          val s1 = x.toSquare
          val s2 = y.toSquare
          val xMin = List(s1.getUpperLeft._1, s2.getUpperLeft._1).min
          val yMin = List(s1.getUpperLeft._2, s2.getUpperLeft._2).min
          val xMax = List(s1.getLowerRight._1, s2.getLowerRight._1).max
          val yMax = List(s1.getLowerRight._2, s2.getLowerRight._2).max
          Square((xMin, yMin), (xMax, yMax)).asInstanceOf[Square]
        }
      }
    }

    fIndex = KMeans(fShapeList, 1)
    bIndex = KMeans(bShapeList, 1)

    (fIndex, bIndex, fMap, bMap)
  }  
}

object SimpleMapping{
  def apply(mapping: List[(List[(Int, Int)], List[(Int, Int)])]) = {
    val (fIndex, bIndex) = buildSimpleIndex(mapping)
    new SimpleMapping(fIndex, bIndex)
  }

  def buildSimpleIndex(mapping: List[(List[(Int, Int)], List[(Int, Int)])]) = {
    /*initialize index*/
    var fIndex: Map[(Int, Int), List[List[(Int, Int)]]] = Map()
    var bIndex: Map[(Int, Int), List[List[(Int, Int)]]] = Map()
    /*build index*/
    mapping.map{
      case (l1, l2) => {
        l1.map{
          key => {
            if(fIndex.contains(key)){
              fIndex(key) = fIndex(key) :+ l2
            }
            else{
              fIndex += key->List(l2)
            }
          }
        }
        l2.map{
          key => {
            if(bIndex.contains(key)){
              bIndex(key) = bIndex(key) :+ l1
            }
            else{
              bIndex += key->List(l1)
            }
          }
        }
      }
    }
    (fIndex, bIndex)
  }
}

object OneManyMapping{
  def apply(mapping: List[(List[(Int, Int)], List[(Int, Int)])]) = {
    val (fIndex, bIndex) = buildOneManyIndex(mapping)
    new OneManyMapping(fIndex, bIndex)
  }

  def buildOneManyIndex(mapping: List[(List[(Int, Int)], List[(Int, Int)])]) = {
    /*initialize index*/
    var firstFIndex: Map[(Int, Int), List[String]] = Map() 
    var secondFIndex: Map[String,List[(Int, Int)]] = Map()
    var firstBIndex: Map[(Int, Int), List[String]] = Map() 
    var secondBIndex: Map[String,List[(Int, Int)]] = Map()
  
    /*build index*/
    mapping.zipWithIndex.map{
      case ((l1, l2), index) => {
        /*build fIndex*/
        val hValue = index.toString
        secondFIndex += hValue->l2
        l1.map{
          key => {
            if(firstFIndex.contains(key)){
              firstFIndex(key) = firstFIndex(key) :+ hValue
            }
            else{
              firstFIndex += key->List(hValue)
            }            
          }
        }
        /*build bIndex*/
        secondBIndex += hValue->l1
        l2.map{
          key => {
            if(firstBIndex.contains(key)){
              firstBIndex(key) = firstBIndex(key) :+ hValue
            }
            else{
              firstBIndex += key->List(hValue)
            } 
          }
        }
      }
    }
    /*return index*/    
    ((firstFIndex, secondFIndex), (firstBIndex, secondBIndex))
  }
}
