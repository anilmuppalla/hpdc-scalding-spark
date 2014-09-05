import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('company, 'size))
        .limit(2)
        .write(Tsv("output.tsv"))
}
