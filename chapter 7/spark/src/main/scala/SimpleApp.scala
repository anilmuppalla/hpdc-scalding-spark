/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val input = sc.textFile("in.txt")
    //var arr = ArrayBuffer.empty[Int]
    val x = input
        .map( line => {
            val parts = line.split(',')
            (parts(0).toDouble)
        })

    x.saveAsTextFile()
    val y = input
        .map( line => {
            val parts = line.split(',')
            (parts(1).toDouble)
        })

    val xy = input
        .map( line => {
            val parts = line.split(',')
            (parts(0).toDouble * parts(1).toDouble)
        })

    val slope = (xy.mean - (x.mean * y.mean)) / x.variance

    val intercept = y.mean - slope * x.mean

    println (slope, intercept)
}
}
