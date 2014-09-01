import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val input = sc.textFile("in.txt")

    val x = input
        .map( line => {
            val parts = line.split(',')
            (parts(0).toDouble)
        })

    val y = input
        .map( line => {
            val parts = line.split(',')
            (parts(1).toDouble)
        })

    var theta0 : Double = 0.0
    var theta1 : Double = 0.0
    var predictions = x.map( line => {
        var temp = theta0 + line * theta1
        (temp)
    })

    for( i <- 1 to 2 ){

        predictions = x.map( line => {
            (theta0 + line * theta1)
        })

        var errors1 = predictions.subtract(y)
        theta0 = theta0 + 1.0
        theta1 = theta1 + 1.0


    }
        var result = predictions.collect()
        println("##########################")
        println(theta0, theta1)
        println("##########################")
        for (i <- result){
            println(i)
        }
        println("##########################")



}
}
