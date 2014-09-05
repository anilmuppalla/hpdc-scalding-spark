import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val pipe1 = Tsv(args("in1"),('shape, 'len, 'wid))
        .read
        .groupBy('shape){
            _.dot[Int]('len, 'wid, 'len_dot_wid)
        }
    .write(Tsv("output.tsv"))
}
