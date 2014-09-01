import com.twitter.scalding._
class LRBGDJob(args: Args) extends Job(args) {
    val JOB_COUNT = args("jobCount").toInt

        override def next: Option[Job] = {
                  val nextArgs = args + ("input", Some(args("output"))) +
                    ("temp", Some(args("output"))) +
                    ("output", Some(args("temp"))) +
                    ("jobCount", Some((JOB_COUNT - 1).toString))
              //try again to get under the error
              if ((JOB_COUNT > 1)) {
              Some(clone(nextArgs))
            } else {
              None
            }
          }


    val input = Tsv(args("input"), ('x, 'y, 't0, 't1))
        .read


    /*val prediction = init
        .map(('x, 't0, 't1) -> ('prediction)){
            f : (Double, Double, Double) =>
            val(x, t0, t1) = f
            (t0 + (t1 * x))
        }
        .write(Tsv("lrprediction.tsv"))
    */

    val errors = input
        .map(('x, 'y, 't0, 't1) -> ('e1, 'e2)){
            f : (Double, Double, Double, Double) =>
            val (x, y, t0, t1) = f
            val prediction = (t0 + (t1 * x))
            val e1 = prediction - y
            val e2 = (prediction - y) * x
            (e1, e2)
        }
    //    .write(Tsv("lrerrors.tsv"))

    val sums = errors
          .groupAll{
              _.sum[Double]('e1 -> 'se1)
              .sum[Double]('e2 -> 'se2)
          }
    //      .write(Tsv("sums.tsv"))


    val errorswithsum = errors
        .crossWithTiny(sums)
    //    .write(Tsv("errorswithsum.tsv"))

    val newthetas = errorswithsum
        .map(('t0, 't1, 'se1, 'se2) -> ('nt0, 'nt1)){
            f : (Double, Double, Double, Double) =>
            val(t0, t1, se1, se2) = f
            val nt0 = t0 - (0.01) * (1.0 / 97) * se1
            val nt1 = t1 - (0.01) * (1.0 / 97) * se2
            (nt0, nt1)
        }
        .project('x, 'y, 'nt0, 'nt1)
        //.rename(('nt0, 'nt1)->('t0, 't1))
        .write(Tsv(args("output")))

    /*
    errors
        .map(('x, 'y, 'e1, 'e2) -> ('x , 'y, 'e1, 'e2)){
            f : (Double, Double, Double, Double) =>
            val (x, y, t0, t1) = f
            val prediction = (t0 + (t1 * x))
            val e1 = prediction - y
            val e2 = (prediction - y) * x
            (x, y, e1, e2)
        }
        .write(Tsv("lrnewerrors.tsv"))
    */
}
