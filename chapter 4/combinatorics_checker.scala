import com.twitter.scalding._
import com.twitter.scalding.mathematics._
class testJob(args: Args) extends Job(args) {
    val perm = TextLine("permutations.txt")
        .groupAll{
            _.size
        }
        .debug
        .write(TextLine("poutput.tsv"))

    val comb = TextLine("combinations.txt")
        .groupAll{
            _.size
        }
        .debug
        .write(TextLine ("coutput.tsv"))
}
