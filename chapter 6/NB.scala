import com.twitter.scalding._
import com.twitter.algebird._
//import cascading.pipe.Pipe
//import cascading.flow.FlowDef

class NBTestJob(args: Args) extends Job(args) {
  val input = args("input")
  val output = args("output")

  val iris = Tsv(input, ('id, 'class, 'sepalLength, 'sepalWidth, 'petalLength, 'petalWidth))
    .read

  val irisMelted = iris
    .unpivot(('sepalLength, 'sepalWidth, 'petalLength, 'petalWidth) -> ('feature, 'score))
    //.write(Tsv(output))

  // Pick every id that is not divisble by 3
  val irisTrain = irisMelted.filter('id){id: Int => (id % 3) != 0}.discard('id)
    //.write(Tsv(output))  

  val irisTest = irisMelted
    .filter('id){id: Int => (id % 3) ==0}
    .discard('class)
    //.write(Tsv(output))

  val counts = irisTrain.groupBy('class) { _.size('classCount).reducers(10) }
    //.write(Tsv(output))
  val totSum = counts.groupAll(_.sum[Double]('classCount -> 'totalCount))
   // .write(Tsv(output))

  val prClass = 
  counts
      .crossWithTiny(totSum)
      .mapTo(('class, 'classCount, 'totalCount) -> ('class, 'classPrior, 'classCount)) {
        x : (String, Double, Double) => (x._1, math.log(x._2 / x._3), x._2)
      }
      .discard('classCount)
      //.write(Tsv(output))

  val prFeatureClass = 
  irisTrain
      .groupBy('feature, 'class) {
        _.sizeAveStdev('score -> ('featureClassSize, 'theta, 'sigma))
         .reducers(10)
      }
      //.write(Tsv(output))
  val model = 
  irisTrain    
    .joinWithSmaller('class -> 'class, prClass, reducers=10)
    //.write(Tsv(output))
    .joinWithSmaller(('class, 'feature) -> ('class, 'feature), prFeatureClass, reducers=10)    
    //.write(Tsv(output))
    .mapTo(('class, 'classPrior, 'feature, 'featureClassSize, 'theta, 'sigma) ->
             ('class, 'feature, 'classPrior, 'theta, 'sigma)) {
        values : (String, Double, String, Double, Double, Double) =>
        val (classId, classPrior, feature, featureClassSize, theta, sigma) = values
        (classId, feature, classPrior, theta, math.pow(sigma, 2))
      }
    //.write(Tsv(output))  

    val joined = irisTest
      .skewJoinWithSmaller('feature -> 'feature, model, reducers=10)
      //.write(Tsv(output))

    def _gaussian_prob(theta : Double, sigma : Double, score : Double) : Double = {
    // from sklearn:
    // n_ij = - 0.5 * np.sum(np.log(np.pi * self.sigma_[i, :]))
    //     n_ij -= 0.5 * np.sum(((X - self.theta_[i, :]) ** 2) /
    //                          (self.sigma_[i, :]), 1)
    // val (theta, sigma, score) = values
    val outside = -0.5 * math.log(math.Pi * sigma)
    val expo = 0.5 * math.pow(score - theta, 2) / sigma
    outside - expo
  }

    

    val result = joined
      .map(('theta, 'sigma, 'score) -> 'evidence) {
        values : (Double, Double, Double) => _gaussian_prob(values._1, values._2, values._3)}
      .project('id, 'class, 'classPrior, 'evidence)
      .groupBy('id, 'class) {
        _.sum[Double]('evidence -> 'sumEvidence)
         .max('classPrior)
      }
      //.write(Tsv(output))
      .mapTo(('id, 'class, 'classPrior, 'sumEvidence) -> ('id, 'class, 'logLikelihood)) {
        values : (String, String, Double, Double) =>
        val (id, className, classPrior, sumEvidence) = values
        (id, className, classPrior + sumEvidence)
      }
      //.write(Tsv(output))
      .groupBy('id) {
        _.sortBy('logLikelihood)
         .reverse
         .take(1)
         .reducers(10)
      }
      .rename(('id, 'class) -> ('id2, 'classPred))
      //.write(Tsv(output))

      val results = iris
        .leftJoinWithTiny('id -> 'id2, result)
        .discard('id2)
        .map('classPred -> 'classPred) {x: String => Option(x).getOrElse("")}
        .project('id, 'class, 'classPred, 'sepalLength, 'petalLength)
        .write(Tsv(output))

} 
