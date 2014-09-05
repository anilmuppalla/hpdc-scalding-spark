import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('student, 'marks))
        .groupBy('student){
            _.max('marks -> 'max)
            .min('marks -> 'min)
            }
        .write(Tsv("output.tsv"))
}
