import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val pipe1 = Tsv(args("in1"),('x, 'y))
        .read
        .map(('x, 'y)-> 'div){
            x : (Int, Int) => x._1 / x._2
        }
        .addTrap(Tsv("output-trap.tsv"))
        .write(Tsv("output.tsv"))
}
