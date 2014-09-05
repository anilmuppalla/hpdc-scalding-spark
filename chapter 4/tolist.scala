import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('word, 'class))
        .groupBy('class){
            _.toList[String]('list)
            .size}
        .write(Tsv("output.tsv"))
}
