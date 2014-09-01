import com.twitter.scalding._
class LRJob(args: Args) extends Job(args) {

    val input = Csv("in.csv", ",",  ('x, 'y))
        .read
        .map(('x, 'y) -> 'xy){
            f: (Double, Double) =>
            val (x,y) = f
            (x * y)
        }
        .write(Tsv("out.tsv"))

    val MVofx = input
        .groupAll{
            _.sizeAveStdev('x -> ('count, 'xmean, 'xsigma))
            }
        .map('xsigma -> 'xvariance){
            (xsigma :Double) => (math.pow(xsigma,2))
        }
        .project('xmean, 'xvariance)
        .write(Tsv("MVX.tsv"))

    val MVofy = input
        .groupAll{
            _.sizeAveStdev('y -> ('count, 'ymean, 'ysigma))
            }
        .map('ysigma -> 'yvariance){
            (ysigma :Double) => (math.pow(ysigma,2))
        }
        .project('ymean)
        .write(Tsv("MVY.tsv"))

    val MVofxy = input
        .groupAll{
            _.sizeAveStdev('xy -> ('count, 'xymean, 'xysigma))
            }
        .project('xymean)
        .write(Tsv("MVXY.tsv"))

    val finput = MVofx
        .crossWithTiny(MVofy)
        .crossWithTiny(MVofxy)
        .map(('xmean, 'xvariance, 'ymean, 'xymean) -> 'slope){
            f: (Double, Double, Double, Double) =>
            val (mx, vx, my, mxy) = f
            ( (mxy - ( mx * my )) / vx)
        }
        .map(('xmean, 'ymean, 'slope) -> 'intercept){
            f : (Double, Double, Double) =>
            val(mx, my, slope) = f
            (my - (slope * mx))
        }
        .project('slope, 'intercept)
        .write(Tsv("final.tsv"))


/*

    val finput = input
        .crossWithTiny(MVofx)
        .crossWithTiny(MVofy)
*/
}
