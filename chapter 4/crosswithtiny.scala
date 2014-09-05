import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val pipe1 = Tsv(args("in1"),('title))
        .read
    val pipe2 = Tsv(args("in2"), ('location))
        .read

    pipe1.crossWithTiny(pipe2).write(Tsv("output.tsv"))
}
