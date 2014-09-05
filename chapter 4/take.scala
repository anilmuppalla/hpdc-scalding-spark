import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('student, 'marks))
        .read
        .map('marks -> 'marksInt){ x : Int => x}
        .discard('marks)
        .groupAll{
            _.sortBy('marksInt).reverse
            .takeWhile('marksInt){ x : Int => x > 100}
        }
        .write(Tsv("output.tsv"))
}
