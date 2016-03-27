package lineage

import breeze.linalg._
import org.scalatest.FunSuite
import pipelines.Logging
import utils.ImageMetadata

class QueryRuleSuite extends FunSuite with Logging {
  test("Collapse Query Rule HToL Test"){
    val m1 = DenseMatrix.zeros[Double](3,2)
    val v1 = DenseVector.zeros[Double](3)
    
    val keys = List(Coor(0,0), Coor(0,1), Coor(1,0), Coor(1,1), Coor(2,0), Coor(2,1))
    val rule = CollapseQueryRule(SubSpace(m1), SubSpace(v1), 1, keys)

    assert(rule.isTotal == true)
    assert(rule.reduce == List(Coor(0), Coor(1), Coor(2)))
  }

  test("Collapse Query Rule LToH Test"){
    val m1 = DenseMatrix.zeros[Double](3,2)
    val v1 = DenseVector.zeros[Double](3)
    
    val keys = List(Coor(0), Coor(1), Coor(2))
    val rule = CollapseQueryRule(SubSpace(v1), SubSpace(m1), 1, keys)

    assert(rule.isTotal == true)
    assert(rule.reduce == List(Coor(0), Coor(1), Coor(2)))
  }

  test("Flatten Query Rule HToL Test"){
    val m1 = DenseMatrix.zeros[Double](3,2)
    val v1 = DenseVector.zeros[Double](6)
    
    val keys = List(Coor(0,0), Coor(0,1), Coor(1,0), Coor(1,1), Coor(2,0), Coor(2,1))
    val rule = FlattenQueryRule(SubSpace(m1), SubSpace(v1), 1, keys)

    assert(rule.isTotal == true)
  } 

  test("Flatten Query Rule LToH Test"){
    val m1 = DenseMatrix.zeros[Double](3,2)
    val v1 = DenseVector.zeros[Double](6)
    
    val keys = List(Coor(0), Coor(1), Coor(2), Coor(3), Coor(4), Coor(5))
    val rule = FlattenQueryRule(SubSpace(v1), SubSpace(m1), 1, keys)

    assert(rule.isTotal == true)
  } 
}