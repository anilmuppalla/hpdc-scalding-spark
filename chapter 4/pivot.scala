import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('course, 'semester, 'students))
        .read
        .groupBy('course) {
          _.pivot(('semester, 'students) -> ('sem1, 'sem2, 'sem3), 0)
        }
        .write(Tsv("output.tsv"))

    input
       .unpivot(('sem1, 'sem2, 'sem3)-> (('semester, 'students)))
       .write(Tsv("unpivot.tsv"))

}
