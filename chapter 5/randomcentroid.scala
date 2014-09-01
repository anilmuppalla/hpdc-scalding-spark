import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.mahout.clustering.kmeans.{RandomSeedGenerator, KMeansDriver}
import org.apache.mahout.common.distance.EuclideanDistanceMeasure

import com.twitter.scalding._
import org.apache.mahout.math.{NamedVector, DenseVector, VectorWritable}
import com.twitter.scalding.IterableSource
import org.apache.mahout.common.HadoopUtil
import org.apache.mahout.clustering.iterator.ClusterWritable

object RCJob extends App{
import Dsl._

println("Generating random centroid to use as seed in K-Means")

    val conf = new Configuration
    conf.set("io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization," +
      "org.apache.hadoop.io.serializer.WritableSerialization")
    val inputClustersPath = new Path("data/kmeans/random_centroids")
    val distanceMeasure = new EuclideanDistanceMeasure
    val vectorsPath = new Path("data/kmeans/mahout_vectors")
    RandomSeedGenerator.buildRandom(conf, vectorsPath, inputClustersPath, 1, distanceMeasure)
}
