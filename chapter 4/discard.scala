import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('product, 'price))
        .discard('product)
        .write(Tsv("output.tsv"))
}
