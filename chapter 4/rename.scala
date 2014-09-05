import com.twitter.scalding._

class testJob(args: Args) extends Job(args) {
    val input = Tsv(args("input"), ('company, 'size))
        .rename(('company, 'size) -> ('product, 'inventory))
        .project(('inventory, 'product))
        .write(Tsv("output.tsv"))
}
