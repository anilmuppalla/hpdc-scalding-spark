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

class InputJob(args: Args) extends Job(args) {
	val mahout_vectors = "data/kmeans/mahout_vectors"
	val random_centroids = "data/kmeans/random_centroids"
	val result_cluster = "data/kmeans/result_cluster"
	val result_distances = "data/kmeans/output-distances.tsv"
	val userFeatures = Tsv("input.tsv", ('id, 'x, 'y))
			.read
			.mapTo(('id, 'x, 'y) -> ('user, 'vector)){
				x : (String, String, String) =>
				val user = x._1
				val vector = Array(x._2.toDouble, x._3.toDouble)
				val namedVector = new NamedVector(new DenseVector(vector), user)
				val vectorWritable = new VectorWritable(namedVector)
				println(namedVector.toString)
				(new Text(user), vectorWritable)
			}
			.project('user, 'vector)
			//.write(Tsv("vectorized.tsv"))
			val out = WritableSequenceFile[Text, VectorWritable](mahout_vectors, ('user , 'vector))
			userFeatures.write(out)
		}
