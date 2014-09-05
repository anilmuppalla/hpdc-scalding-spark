import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('location, 'amount))
        .groupBy('location){
            _.sum[Int]('amount -> 'total)
            }
        .write(Tsv("output.tsv"))
}
