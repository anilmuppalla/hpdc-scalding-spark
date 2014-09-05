import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('key, 'product_id, 'rating))
        .read
        .map(('key, 'product_id, 'rating) -> ('key, 'product_id, 'rating)){
            x : (String, Int, Double) =>
            val(key, product_id, rating) = x
            (key, product_id, rating)
        }
        .groupBy('key) {
          _.sortWithTake[(Int, Double)]((('product_id, 'rating), 'top_products), 5) {
          (product_0: (Int, Double), product_1: (Int, Double)) =>
             if (product_0._2 == product_1._2) {
                 product_0._1 < product_1._1
            }
             else {
                 product_0._2 > product_1._2
             }
         }
        }
        .write(Tsv("output.tsv"))
}
