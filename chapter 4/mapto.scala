import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('price,'discount))
        .mapTo(('price, 'discount) -> ('savings)){
            x : (Int, Int) =>
            val(price, discount) = x
            (price - discount)
        }
        .write(Tsv("output.tsv"))
}
