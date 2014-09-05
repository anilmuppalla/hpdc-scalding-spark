import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('animal, 'kind))
        .filterNot('kind) { kind : String => kind == "bird"}
        .write(Tsv("output.tsv"))
}
