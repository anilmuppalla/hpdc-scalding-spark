import com.twitter.scalding._
import com.twitter.scalding.mathematics._
class testJob(args: Args) extends Job(args) {
    val c = Combinatorics
    c.permutations(6, 3).write(Tsv("permutations.txt"))
    c.combinations(5, 2).write(Tsv("combinations.txt"))
}
