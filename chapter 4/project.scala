import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('name, 'age))
        .project('name)
        .write(Tsv("output.tsv"))
}
