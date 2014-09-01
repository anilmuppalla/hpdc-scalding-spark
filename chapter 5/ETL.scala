import org.apache.mahout.clustering.kmeans.{RandomSeedGenerator, KMeansDriver}
import org.apache.mahout.common.distance.EuclideanDistanceMeasure
import org.apache.mahout.math.{NamedVector, DenseVector, VectorWritable}
import org.apache.mahout.common.HadoopUtil
import org.apache.mahout.clustering.iterator.ClusterWritable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}

import com.twitter.scalding._
import com.twitter.scalding.IterableSource

class ETLJob(args: Args) extends Job(args) {

    val mahout_vectors = "data/kmeans/mahout_vectors"
    val random_centroids = "data/kmeans/random_centroids"
    val result_cluster = "data/kmeans/result_cluster"
    val result_distances = "data/kmeans/output-distances.tsv"

  val conf = new Configuration
  conf.set("io.serializations",
    "org.apache.hadoop.io.serializer.JavaSerialization," +
    "org.apache.hadoop.io.serializer.WritableSerialization")
  val inputClustersPath = new Path("data/kmeans/random_centroids")
  val distanceMeasure = new EuclideanDistanceMeasure
  val vectorsPath = new Path("data/kmeans/mahout_vectors")

  RandomSeedGenerator.buildRandom(conf, vectorsPath, inputClustersPath, 3, distanceMeasure)

    HadoopUtil.delete(conf, new Path(result_cluster));

    KMeansDriver.run(
      conf,
      new Path(mahout_vectors),   // INPUT
      new Path(random_centroids),
      new Path(result_cluster),   // OUTPUT_PATH
      0.01,                       //convergence delta
      20,                         // MAX_ITERATIONS
      true,                       // run clustering
      0,                          // cluster classification threshold
      false)

    val finalClusterPath = result_cluster + "/*-final"
      val finalCluster = WritableSequenceFile[IntWritable, ClusterWritable](finalClusterPath, ('clusterId, 'cluster))

      val clusterCenter = finalCluster
          .read
          .map('cluster -> 'center) {
              x: ClusterWritable =>
              val center = x.getValue.getCenter
              println("cluster center -> " + center + " size -> " + center.size)
              center
              }

    val userVectors  = WritableSequenceFile[Text, VectorWritable](mahout_vectors, ('user , 'vector))
    //userFeatures
      .crossWithTiny(clusterCenter)
      .map(('center, 'vector) -> 'distance) {
           x:(DenseVector, VectorWritable) =>
        (new EuclideanDistanceMeasure()).distance(x._1, x._2.get)
       }
      .write(Tsv(result_distances))
}
