import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val pipe1 = Tsv(args("in1"),('student, 'marks))
        .read
        .map('marks -> 'marksInt){
            x : Int => x
        }
        .discard('marks)
        .groupAll{
            _.sortBy('marksInt).reverse
        }
        .sample(0.8)
        .debug
        .write(Tsv("output.tsv"))
}
