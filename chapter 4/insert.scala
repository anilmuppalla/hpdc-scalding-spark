import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('product))
        .insert(('country), ("USA"))
        .write(Tsv("output.tsv"))
}
