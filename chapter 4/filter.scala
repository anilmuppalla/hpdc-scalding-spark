import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('animal, 'kind))
        .filter('kind) { kind : String => kind == "bird"}
        .write(Tsv("output.tsv"))
}
