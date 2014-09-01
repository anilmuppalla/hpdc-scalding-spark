import com.twitter.scalding._

class MovieSimilarities(args : Args) extends Job(args) {

  val PRIOR_COUNT = 10
  val PRIOR_CORRELATION = 0
  val INPUT_FILENAME = "ua.base"
  val ratings =
    Tsv(INPUT_FILENAME).read
      .mapTo((0, 1, 2) -> ('user, 'movie, 'rating)) {
        fields : (Int, Int, Double) => fields
      }
  val ratingsWithSize =
    ratings
      .groupBy('movie) { _.size('numRaters) }
      .rename('movie -> 'movieX)
      .joinWithLarger('movieX -> 'movie, ratings).discard('movieX)
    
  val ratings2 =
    ratingsWithSize
      .rename(('user, 'movie, 'rating, 'numRaters) -> ('user2, 'movie2, 'rating2, 'numRaters2))

  val ratingPairs =
    ratingsWithSize
      .joinWithSmaller('user -> 'user2, ratings2)
      .filter('movie, 'movie2) { movies : (String, String) => movies._1 < movies._2 }
      .project('movie, 'rating, 'numRaters, 'movie2, 'rating2, 'numRaters2)
      .write(Tsv("ratingPairs.tsv"))


val vectorCalcs =
  ratingPairs
    .map(('rating, 'rating2) -> ('ratingProd, 'ratingSq, 'rating2Sq)) {
      ratings : (Double, Double) =>
      (ratings._1 * ratings._2, scala.math.pow(ratings._1, 2), scala.math.pow(ratings._2, 2))
    }
    .groupBy('movie, 'movie2) {
      _
      .spillThreshold(500000)
        .size // length of each vector
        .sum[Double]('ratingProd -> 'dotProduct)
        .sum[Double]('rating -> 'ratingSum)
        .sum[Double]('rating2 -> 'rating2Sum)
        .sum[Double]('ratingSq -> 'ratingNormSq)
        .sum[Double]('rating2Sq -> 'rating2NormSq)
        .max('numRaters) // Just an easy way to make sure the numRaters field stays.
        .max('numRaters2)
    }
    //.write(Tsv("vectorCalcs.tsv"))
    val similarities =
      vectorCalcs
        .map(('size, 'dotProduct, 'ratingSum, 'rating2Sum, 'ratingNormSq, 'rating2NormSq, 'numRaters, 'numRaters2) ->
          ('correlation, 'regularizedCorrelation, 'cosineSimilarity, 'jaccardSimilarity)) {

          fields : (Double, Double, Double, Double, Double, Double, Double, Double) =>

          val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields

          val corr = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
          val regCorr = regularizedCorrelation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, PRIOR_COUNT, PRIOR_CORRELATION)
          val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))
          val jaccard = jaccardSimilarity(size, numRaters, numRaters2)

          (corr, regCorr, cosSim, jaccard)
        }

    similarities
      .filterNot('regularizedCorrelation){
          regularizedCorrelation : Double => regularizedCorrelation.equals(Double.NaN)
      }
      .groupBy('movie){
          _.sortBy('regularizedCorrelation)
      }
      .project('movie, 'movie2, 'correlation, 'regularizedCorrelation, 'cosineSimilarity, 'jaccardSimilarity, 'size, 'numRaters, 'numRaters2)
      .write(Tsv("./outputuabase.tsv"))

  def correlation(size : Double, dotProduct : Double, ratingSum : Double,
    rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double) = {

    val numerator = size * dotProduct - ratingSum * rating2Sum
    val denominator = scala.math.sqrt(size * ratingNormSq - ratingSum * ratingSum) * scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)

    numerator / denominator
  }

  def regularizedCorrelation(size : Double, dotProduct : Double, ratingSum : Double,
    rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double,
    virtualCount : Double, priorCorrelation : Double) = {

    val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
    val w = size / (size + virtualCount)

    w * unregularizedCorrelation + (1 - w) * priorCorrelation
  }
  def cosineSimilarity(dotProduct : Double, ratingNorm : Double, rating2Norm : Double) = {
    dotProduct / (ratingNorm * rating2Norm)
  }

  def jaccardSimilarity(usersInCommon : Double, totalUsers1 : Double, totalUsers2 : Double) = {
    val union = totalUsers1 + totalUsers2 - usersInCommon
    usersInCommon / union
  }
}
