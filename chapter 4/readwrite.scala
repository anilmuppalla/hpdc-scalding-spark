import com.twitter.scalding._
import com.twitter.scalding.mathematics._
class testJob(args: Args) extends Job(args) {
    val input = TextLine("README.md")
        .read
        .write(TextLine("readwrite.txt"))
}
