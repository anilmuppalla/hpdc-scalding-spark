import com.twitter.scalding._

class MovieSimilarities(args : Args) extends Job(args) {
    val input = Tsv("outputuabase.tsv", ('movie, 'movie2, 'correlation,
        'regularizedCorrelation, 'cosineSimilarity, 'jaccardSimilarity, 'size, 'numRaters, 'numRaters2))
        .read
        .filter('movie){ movie : String => movie.equals("50")}
        .groupBy('movie){
            _.sortBy('regularizedCorrelation)
        }
        .write(Tsv("outwithoutNAN.tsv"))
}
