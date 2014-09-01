import com.twitter.scalding._

class MovieSimilarities(args : Args) extends Job(args) {
    val input = Tsv("outwithoutNAN.tsv", ('movie, 'movie2, 'correlation,
        'regularizedCorrelation, 'cosineSimilarity, 'jaccardSimilarity, 'size, 'numRaters, 'numRaters2))
        .read
    val maped = Tsv("movienames.txt", ('mid, 'mname))
        .read
    input
        .joinWithSmaller('movie -> 'mid, maped)
        .discard('movie)
        .rename(('mname, 'mid) -> ('movie, 'movieid))
        .joinWithSmaller('movie2 -> 'mid, maped)
        .discard('movie2)
        .rename('mname -> 'movie2)
        .groupBy('movieid){
            _.sortBy('correlation).reverse
        }
        .project('movie, 'movieid, 'mid, 'movie2, 'correlation)
        .write(Tsv("changedoutput.tsv"))
    }
