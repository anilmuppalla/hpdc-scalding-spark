import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('country, 'population))
        .groupAll{
            _.sortBy('country)
        }
        .write(Tsv("output.tsv"))
}
