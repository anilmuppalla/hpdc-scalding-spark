import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('customer, 'product, 'bought))
        .groupBy('customer){
            _.foldLeft('bought -> 'bought)(false){
                (prev : Boolean, current : Boolean) =>
                    prev || current
            }
        }
        .write(Tsv("output.tsv"))
}
