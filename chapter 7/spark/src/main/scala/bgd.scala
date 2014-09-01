import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object BGD {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val input = sc.textFile("in.txt")

    var m = input.count().toDouble

    var theta0 : Double = 0.0
    var theta1 : Double = 0.0

    var e1sum : Double = 0.0
    var e2sum : Double = 0.0

    val xy = input
        .map( line => {
            val parts = line.split(',')
            (parts(0).toDouble, parts(1).toDouble)
        })

    for( i <- 1 to 4250){
        e1sum = xy.map(
            line => {
                var prediction = theta0 + theta1 * line._1
                var diff = prediction - line._2
                (diff)
        }).sum()

        e2sum = xy.map(
            line => {
                var prediction = theta0 + theta1 * line._1
                var diff = (prediction - line._2) * line._1
                (diff)
        }).sum()

        theta0 = theta0 - 0.01 * ( 1.0 / m ) * e1sum
        theta1 = theta1 - 0.01 * ( 1.0 / m ) * e2sum
    }
    println(theta0, theta1)

}
}
