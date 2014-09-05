import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('cust, 'product))
        .groupBy('cust){_.size}
        .write(Tsv("output.tsv"))
}
