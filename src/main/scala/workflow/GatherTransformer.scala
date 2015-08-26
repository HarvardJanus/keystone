package workflow

import org.apache.spark.rdd.RDD
import workflow._

import scala.reflect.ClassTag

//private[workflow] class GatherTransformer[T] extends TransformerNode[Seq[T]] {
class GatherTransformer[T] extends TransformerNode[Seq[T]] {
  def transform(dataDependencies: Seq[_], fitDependencies: Seq[TransformerNode[_]]): Seq[T] = dataDependencies.map(_.asInstanceOf[T])

  def transformRDD(dataDependencies: Seq[RDD[_]], fitDependencies: Seq[TransformerNode[_]]): RDD[Seq[T]] = {
    dataDependencies.map(_.asInstanceOf[RDD[T]].map(t => Seq(t))).reduceLeft((x, y) => {
      x.zip(y).map(z => z._1 ++ z._2)
    })
  }

  def transformRDDWithLineage(dataDependencies: Seq[RDD[_]], fitDependencies: Seq[TransformerNode[_]], tag: String): RDD[Seq[T]] = {
  	val out = transformRDD(dataDependencies, fitDependencies)
    out.cache()
  	val lineage = GatherLineage(dataDependencies.map(_.asInstanceOf[RDD[T]]), out, this)
  	lineage.save(tag)
  	println("collecting lineage for Transformer "+this.label)
  	out
  }
}