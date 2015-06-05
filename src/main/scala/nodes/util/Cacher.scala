package nodes.util

import org.apache.spark.rdd.RDD
import pipelines.Logging
import workflow.Transformer

import scala.reflect.ClassTag

/**
 * Caches the intermediate state of a node. Follows Spark's lazy evaluation conventions.
 * @param name An optional name to set on the cached output. Useful for debugging.
 * @tparam T Type of the input to cache.
 */
class Cacher[T: ClassTag](name: Option[String] = None) extends Transformer[T,T] with Logging {
  override def apply(in: RDD[T]): RDD[T] = {
    logInfo(s"CACHING ${in.id}")
    name match {
      case Some(x) => in.cache().setName(x)
      case None => in.cache()
    }
  }

  def apply(in: T): T = in
}
