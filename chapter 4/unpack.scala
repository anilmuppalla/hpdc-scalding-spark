import com.twitter.scalding._

case class Company(companyID : String, size : Long =0, revenue : Int = 0)

class testJob(args: Args) extends Job(args) {

    val sampleinput = List(
        Company("Dell",40000,15),
        Company("Facebook",23000,32),
        Company("Google",47000,40),
        Company("Apple",17000,34))

    val input = IterableSource[(Company)] (sampleinput, ('company))
        .unpack[Company]('company -> ('companyID, 'size, 'revenue))
        .write(Tsv("output.tsv"))
}
