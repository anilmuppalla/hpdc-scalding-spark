import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('name, 'age, 'sex))
        .groupBy('sex){ _.sizeAveStdev('age -> ('count, 'mean, 'stdev))}
        .write(Tsv("output.tsv"))
}
