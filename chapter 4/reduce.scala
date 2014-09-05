import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('location, 'amount))
        .groupBy('location){
            _.reduce('amount -> 'total){
                (temp : Int, amount : Int) => temp + amount
            }
        }
        .write(Tsv("output.tsv"))
}
