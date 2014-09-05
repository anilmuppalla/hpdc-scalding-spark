import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('numbers))
        .read
        .mapTo('numbers -> 'numbersInt){ x : Int => x}
        .groupAll{
            _.sortBy('numbersInt)
            .drop(3)
        }
        .write(Tsv("output.tsv"))
}
