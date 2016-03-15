package nodes.images.external

import breeze.linalg._
import lineage._
import nodes.images.FisherVectorInterface
import nodes.learning.{GaussianMixtureModelEstimator, GaussianMixtureModel}
import org.apache.spark.rdd.RDD
import utils.MatrixUtils
import utils.external.EncEval
import workflow.{Transformer, Estimator}

/**
 * Implements a wrapper for the `enceval` Fisher Vector implementation.
 *
 * @param gmm A trained Gaussian Mixture Model
 */
case class FisherVector(
    gmm: GaussianMixtureModel)
  extends FisherVectorInterface {

  @transient lazy val extLib = new EncEval()

  val numDims = gmm.means.rows
  val numCentroids = gmm.means.cols
  val numFeatures = numDims * numCentroids * 2

  override def apply(in: DenseMatrix[Float]): DenseMatrix[Float] = {
    val means = convert(gmm.means, Float).toArray
    val vars = convert(gmm.variances, Float).toArray
    val wts = convert(gmm.weights, Float).toArray

    val fisherVector = extLib.calcAndGetFVs(means, numDims, numCentroids,
      vars, wts, in.toArray)

    new DenseMatrix(numDims, numCentroids*2, fisherVector)
  }

  override def saveLineageAndApply(in: RDD[DenseMatrix[Float]], tag: String): RDD[DenseMatrix[Float]] = {
    val stamp1 = System.nanoTime()
    val out = in.map(apply)
    out.cache()
    out.count()
    val stamp2 = System.nanoTime()
    val lineage = AllLineage(in, out, this)
    lineage.saveMapping(tag)
    val stamp3 = System.nanoTime()
    lineage.saveOutput(tag)
    //println("collecting lineage for Transformer "+this.label+"\t mapping: "+lineage.qBackward(List(Coor(0,0,0))).size)
    val stamp4 = System.nanoTime()
    println(s"Transformer $tag: exec: ${(stamp2 - stamp1)/1e9}s, mapping: ${(stamp3-stamp2)/1e9}s, output: ${(stamp4-stamp3)/1e9}s")
    out
  }
}

/**
 * Trains an `enceval` Fisher Vector implementation, via
 * estimating a GMM by treating each column of the inputs as a separate
 * DenseVector input to [[GaussianMixtureModelEstimator]]
 *
 * TODO: Pending philosophical discussions on how to best make it so you can
 * swap in GMM, KMeans++, etc. for Fisher Vectors. For now just hard-codes GMM here
 *
 * @param k Number of centers to estimate.
 */
case class GMMFisherVectorEstimator(k: Int) extends Estimator[DenseMatrix[Float], DenseMatrix[Float]] {
  protected def fit(data: RDD[DenseMatrix[Float]]): FisherVector = {
    val gmmTrainingData = data.flatMap(x => convert(MatrixUtils.matrixToColArray(x), Double))
    val gmmEst = new GaussianMixtureModelEstimator(k)
    val gmm = gmmEst.fit(gmmTrainingData)
    FisherVector(gmm)
  }
}