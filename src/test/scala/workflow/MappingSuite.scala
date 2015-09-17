package workflow

import archery._
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.Logging
import utils.{ImageUtils, TestUtils}
import utils.{MultiLabeledImage, Image, LabeledImage, ImageMetadata}

import java.io._
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.io.Source

class MappingSuite extends FunSuite with Logging {
  test("OneToOne Vector-to-Vector Mapping Test"){
    val mapping = new OneToOneMapping(5, 1, 5, 1, 1)
    val fResult = mapping.qForward(Some(0))
    assert(fResult == List(0))

    val bResult = mapping.qBackward(Some(4))
    assert(bResult == List(4))

    //test out of bound query
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(5))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(5))
    }

    //test wrong dimensional query
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0,0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0,0))
    }
  }

  test("OneToOne Matrix-to-Matrix Mapping Test"){
    val mapping = new OneToOneMapping(5, 5, 5, 5, 1)
    val fResult = mapping.qForward(Some(0,0))
    assert(fResult == List((0,0)))

    val bResult = mapping.qBackward(Some(4,4))
    assert(bResult == List((4,4)))

    //out of bound query
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(5, 5))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(5, 5))
    }

    //wrong dimensional key
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0))
    }
  }

  test("OneToOne Matrix-to-Vector Mapping Test"){
    val mapping = new OneToOneMapping(5, 5, 25, 1, 1)
    val fResult = mapping.qForward(Some(0,0))
    assert(fResult == List(0))

    val bResult = mapping.qBackward(Some(24))
    assert(bResult == List((4,4)))

    //out of bound query
    intercept[java.lang.IllegalArgumentException] {
      val tResult = mapping.qForward(Some(5, 5))
    }

    intercept[java.lang.IllegalArgumentException] {
      val tResult = mapping.qBackward(Some(25))
    }

    //wrong dimensional key
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0,0))
    }
  }

  test("OneToOne Seq[Vector]-to-Vector Mapping Test"){
    val mapping = new OneToOneMapping(5, 1, 15, 1, 3)
    val fResult = mapping.qForward(Some(2,4))
    assert(fResult == List(14))

    val bResult = mapping.qBackward(Some(14))
    assert(bResult == List((2,4)))

    //out of bound query
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(2,5))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(15))
    }

    //wrong dimensional key
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0,0))
    }
  }

  test("OneToOne Image-to-Image Mapping test"){
    val meta = new ImageMetadata(2, 2, 2)
    val mapping = new OneToOneMapping(8, 1, 8, 1, 1, Some(meta))
    val fResult = mapping.qForward(Some(0))
    assert(fResult == List(0))

    val bResult = mapping.qBackward(Some(0))
    assert(bResult == List(0))

    //out of bound query
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(8))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(8))
    }

    //wrong dimensional key
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0,0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0,0))
    }
  }

  test("AllToOne Vector-to-Vector Mapping Test"){
    val mapping = new AllToOneMapping(5, 1, 5, 1)
    val fResult = mapping.qForward(Some(2))
    assert(fResult == List(0, 1, 2, 3, 4))

    val bResult = mapping.qForward(Some(4))
    assert(bResult == List(0, 1, 2, 3, 4))

    //out of bound query
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(5))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(5))
    }

    //wrong dimensional key
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0,0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0,0))
    }
  }

  test("AllToOne Matrix-to-Matrix Mapping test"){
    val mapping = new AllToOneMapping(2, 2, 2, 2)
    val fResult = mapping.qForward(Some(0,0))
    assert(fResult == List((0,0), (0,1), (1,0), (1,1)))

    val bResult = mapping.qBackward(Some(0,0))
    assert(bResult == List((0,0), (0,1), (1,0), (1,1)))

    //out of bound query
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(2,2))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(2,2))
    }

    //wrong dimensional key
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0))
    }
  }

  test("AllToOne Image-to-Image Mapping test"){
    val meta = new ImageMetadata(2, 2, 2)
    val mapping = new AllToOneMapping(8, 1, 8, 1, Some(meta))
    val fResult = mapping.qForward(Some(0))
    assert(fResult == List(0))

    val bResult = mapping.qBackward(Some(0))
    assert(bResult == List(0,4))

    //out of bound query
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(8))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(8))
    }

    //wrong dimensional key
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0,0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0,0))
    }
  }

  test("LinCom Vector-to-Vector Mapping test"){
    val mapping = new LinComMapping(2, 1, 2, 1, 2, 2)
    val fResult = mapping.qForward(Some(0))
    assert(fResult == List(0, 1))

    val bResult = mapping.qBackward(Some(0))
    assert(bResult == List((List(0, 1),List((0,0), (1,0)))))

    //out of bound query
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(2))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(2))
    }

    //wrong dimensional key
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0,0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0,0))
    }
  }

  test("LinCom Matrix-to-Matrix Mapping test"){
    val mapping = new LinComMapping(2, 2, 2, 2, 2, 2)
    val fResult = mapping.qForward(Some(0, 0))
    assert(fResult == List((0,0), (0,1)))

    val bResult = mapping.qBackward(Some(0, 0))
    assert(bResult == List((List((0,0), (0,1)),List((0,0), (1,0)))))

    //out of bound query
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(2,2))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(2,2))
    }

    //wrong dimensional key
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0))
    }
  }

  test("Contour Matrix-to-Matrix Mapping test"){
    val c1 = Circle((2,2), 2)
    val c2 = Circle((2,5), 2)
    val s1 = Square((1,2), 1, 1)
    val s2 = Square((1,4), 1, 1)
    val fMap = Map(c1->s1, c2->s2)
    val bMap = Map(s1->c1, s2->c2)
    val mapping = new ContourMapping(fMap, bMap)
    
    val fResult = mapping.qForward(Some(2,2))
    assert(fResult == List(List((0,1), (0,2), (0,3), (1,1), (1,2), (1,3), (2,1), (2,2), (2,3))))

    val bResult = mapping.qBackward(Some(1, 2))
    assert(bResult == List(List((0,2), (1,1), (1,2), (1,3), (2,0), (2,1), (2,2), (2,3), (2,4), (3,1), (3,2), (3,3), (4,2))))

    //wrong dimensional key
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0))
    }
  }

  test("Contour Matrix-to-Matrix Mapping with RTree Index test"){
    val c1 = Circle((2,2), 2)
    val c2 = Circle((2,5), 2)
    val s1 = Square((0,1), (2,3))
    val s2 = Square((0,3), (2,5))
    
    val fRTree: RTree[Shape] = RTree(Entry(c1.toBox, c1), Entry(c2.toBox, c2))
    val bRTree: RTree[Shape] = RTree(Entry(s1.toBox, s1), Entry(s2.toBox, s2))
    val fMap: Map[Shape, Shape] = Map(c1->s1, c2->s2)
    val bMap: Map[Shape, Shape] = Map(s1->c1, s2->c2)

    val mapping = new ContourMappingRTree(fRTree, bRTree, fMap, bMap)
    
    val fResult = mapping.qForward(Some(2,2))
    assert(fResult == List(List((0,1), (0,2), (0,3), (1,1), (1,2), (1,3), (2,1), (2,2), (2,3))))

    val bResult = mapping.qBackward(Some(1,2))
    assert(bResult == List(List((0,2), (1,1), (1,2), (1,3), (2,0), (2,1), (2,2), (2,3), (2,4), (3,1), (3,2), (3,3), (4,2))))

    //multiple results
    val fResult2 = mapping.qForward(Some(2,3))
    assert(fResult2 == List(List((0,1), (0,2), (0,3), (1,1), (1,2), (1,3), (2,1), (2,2), (2,3)), 
      List((0,3), (0,4), (0,5), (1,3), (1,4), (1,5), (2,3), (2,4), (2,5))))

    val bResult2 = mapping.qBackward(Some(2,3))
    assert(bResult2 == List(List((0,2), (1,1), (1,2), (1,3), (2,0), (2,1), (2,2), (2,3), (2,4), (3,1), (3,2), (3,3), (4,2)), 
      List((0,5), (1,4), (1,5), (1,6), (2,3), (2,4), (2,5), (2,6), (2,7), (3,4), (3,5), (3,6), (4,5))))

    //search key in shape.toBox but not actually in shape
    val fResult3 = mapping.qForward(Some(0,0))
    assert(fResult3 == List())

    //wrong dimensional key
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0))
    }
  }

  test("Transpose Matrix-to-Matrix Mapping test"){
    val mapping  = new TransposeMapping(5, 3, 3, 5)
    val fResult = mapping.qForward(Some(1, 2))
    assert(fResult == List((2,1)))

    val bResult = mapping.qBackward(Some(2, 1))
    assert(bResult == List((1,2)))

    //out of bound query
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(5, 3))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(3, 5))
    }

    //wrong dimensional key
    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qForward(Some(0))
    }

    intercept[java.lang.IllegalArgumentException] {
      val result = mapping.qBackward(Some(0))
    }
  }

  test("Misc Mapping with Map test"){
    val map = Map(0L->(0,0), 1L->(1,1))
    val mapping = new MiscMapping(map)

    val fResult = mapping.qForward(Some(0L))
    assert(fResult == List((0,0)))

    val bResult = mapping.qBackward(Some(0L))
    assert(bResult == fResult)
  }
}