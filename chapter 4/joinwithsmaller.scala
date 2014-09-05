import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val pipe1 = Tsv(args("in1"),('studid1, 'sub1))
        .read
    val pipe2 = Tsv(args("in2"), ('studid2, 'sub2))
        .read


    pipe1.joinWithSmaller('studid1 -> 'studid2, pipe2)
        .discard('studid2)
        .write(Tsv("output.tsv"))

}
