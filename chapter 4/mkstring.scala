import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('name, 'chat))
        .groupBy('name){ _.mkString('chat, " ")}
        .write(Tsv("output.tsv"))
}
