import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('name, 'phone))
        .groupAll{_.sortBy('name)}
        .write(Tsv("output.tsv"))
}
