import com.twitter.scalding._

case class Company(companyID : String, size : Long =0, revenue : Int = 0)

class testJob(args: Args) extends Job(args) {

    val sampleinput = List(
        ("Dell",40000L,15),
        ("Facebook",23000L,32),
        ("Google",47000L,40),
        ("Apple",17000L,34))

    val input = IterableSource[(String, Long, Int)] (sampleinput, ('companyID, 'size, 'revenue))
        .pack[Company](('companyID, 'size, 'revenue) -> 'Company)
        .write(Tsv("output.tsv"))
}
