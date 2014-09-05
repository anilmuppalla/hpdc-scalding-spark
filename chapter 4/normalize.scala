import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val pipe1 = Tsv(args("in1"),('player, 'rating))
        .read
        .normalize('rating)
        .write(Tsv("output.tsv"))
}
