import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('studentID, 'birthPlace))
        .groupBy('birthPlace){ _.size}
        .write(Tsv("output.tsv"))
}
