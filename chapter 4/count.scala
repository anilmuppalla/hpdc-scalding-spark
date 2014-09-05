import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('player, 'runs))
        .groupAll{
            _.count(('player, 'runs) -> 'c){
                x : (String, Int) =>
                val (player, runs) = x
                (runs > 40)
                }
            }
        .write(Tsv("output.tsv"))
}
