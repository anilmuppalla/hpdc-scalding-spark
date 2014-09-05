import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('player, 'age))
        .groupAll{ _.average('age)}
        .write(Tsv("output.tsv"))
}
