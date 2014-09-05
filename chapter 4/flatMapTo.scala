import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), 'lines)
        .flatMapTo('lines -> 'word){ line : String => line.split(' ')
        }
        .write(Tsv("output.tsv"))
}
