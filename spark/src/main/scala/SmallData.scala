package org.atchai.test

// Scala
import scala.util.matching.Regex

// Scopt
import scopt.OptionParser

// Spark/Hadoop
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vector
import org.apache.hadoop.conf.Configuration

// MongoDB
import com.mongodb.hadoop.MongoInputFormat
import org.bson.BSONObject

// ElasticSearch
import org.elasticsearch.spark._


object SmallData {

    /**
     * Types
     */
    case class Options (
        mongoUrl: String = "mongodb://127.0.0.1:27017",
        mongoContent: String = "data.comments",
        usersFile: String = "data/users.txt",
        esUrl: String = "http://127.0.0.1:9200",
        esIndex: String = "smalldata-spark",
        esContent: String = "users",
        numTerms: Int = 10
    )


    /**
     * Constants
     */
    val stopWords = Set(
        "com", "http", "twitter", "pic", "me", "de", "you", "ly", "que", "www", "my", "la", "en",
        "via", "bit", "el", "your", "rt", "so", "one", "im", "se", "new", "instagram", "all", "a",
        "have", "do", "es", "like", "https", "co", "when", "up", "un", "get", "te", "we", "dont",
        "por", "what", "from", "its", "can", "who", "con", "para", "los", "mi", "how", "lo", "now",
        "more", "about", "youtu", "good", "want", "go", "una", "si", "see", "jp", "las", "html",
        "our", "us", "he", "ow", "al", "her", "am", "gl", "goo", "tu", "eu", "has", "vine",
        "youtube", "como", "please", "why", "org", "some", "le", "tweet", "bu", "retweet", "ve",
        "yo", "ne", "net", "you're", "too", "na", "his", "them", "mas", "off", "ask", "a", "above",
        "after", "again", "against", "an", "and", "any", "are", "aren't", "as", "at", "be",
        "because", "been", "before", "being", "below", "between", "both", "but", "by", "cannot",
        "could", "couldn", "did", "didn't", "does", "doesn't", "doing", "don't", "down", "during",
        "each", "few", "for", "further", "had", "hadn't", "hasn't", "haven", "having", "he'd",
        "he's", "here", "here", "hers", "herself", "he'll", "him", "how", "i", "i'm", "i've", "if",
        "in", "into", "is", "isn't", "it", "it's", "itself", "let", "most", "mustn't", "myself", "no",
        "nor", "not", "of", "on", "once", "only", "or", "other", "ought", "ours", "out", "over",
        "own", "same", "shan't", "she", "should", "shouldn't", "such", "than", "that", "the",
        "their", "theirs", "themselves", "then", "there", "there's", "these", "they", "they're",
        "they've", "this", "those", "through", "to", "under", "until", "very", "was", "wasn't", "we",
        "were", "where", "which", "while", "whom", "with", "will", "would", "won't", "wouldn't",
        "yours", "yourself", "yourselves"
    )
    val tokenise = new Regex("\\s+")
    val leadingPunctuation = new Regex("^[^a-z0-9#@]+")
    val trailingPunctuation = new Regex("[^a-z0-9]+$")
    val consecutivePunctuation = new Regex("[^a-z0-9]{2,}")


    /**
     * Parse arguments
     */
    def main(args: Array[String]) {

        val parser = new OptionParser[Options]("SmallData") {
            head("SmallData: Common terms")
            arg[String]("<mongo-url>")
                .text("MongoDB server URL")
                .required()
                .action((x, c) => c.copy(mongoUrl = x))
            arg[String]("<mongo-content>")
                .text("MongoDB content DB and collection")
                .required()
                .action((x, c) => c.copy(mongoContent = x))
            arg[String]("<users-file>")
                .text("Path to users file")
                .required()
                .action((x, c) => c.copy(usersFile = x))
            arg[String]("<es-url>")
                .text("ElasticSearch server URL")
                .required()
                .action((x, c) => c.copy(esUrl = x))
            arg[String]("<es-index>")
                .text("ElasticSearch index")
                .required()
                .action((x, c) => c.copy(esIndex = x))
            arg[String]("<es-content>")
                .text("ElasticSearch content type")
                .required()
                .action((x, c) => c.copy(esContent = x))
            arg[Int]("<num-terms>")
                .text("Number of distinctive terms to generate")
                .required()
                .action((x, c) => c.copy(numTerms = x))
        }

        parser.parse(args, Options()).map { opts =>
            run(opts)
        }.getOrElse {
            sys.exit(1)
        }

    }

    /**
     * Run distinctive terms process
     */
    def run(options: Options) {

        // Get Spark config
        val sparkConf = new SparkConf()
            .setAppName("SmallData")
            .set("es.nodes", options.esUrl)

        // Get Spark context
        val ctx = new SparkContext(sparkConf)

        // Get (uid, username)
        val users = ctx.broadcast(
            ctx.textFile(options.usersFile)
                .map {
                    line => val Array(uid, username) = line.split(' ')
                    (uid.toInt, username)
                }
                .collectAsMap
        )

        // Get MongoDB config
        val mongoConfig = new Configuration()
        mongoConfig.set("mongo.input.uri", "%s/%s".format(options.mongoUrl, options.mongoContent))

        // Get (uid, comment) from MongoDB
        val comments = ctx.newAPIHadoopRDD(
                mongoConfig,
                classOf[MongoInputFormat],
                classOf[Object],
                classOf[BSONObject]
            )
            // Remove unknown users
            .filter {
                case (oid, row) => (users.value.contains(row.get("user_id").toString.toInt))
            }
            // Get (uid, content)
            .map {
                case (oid, row) => (
                    row.get("user_id").toString.toInt,
                    row.get("body").toString
                )
            }

        // Get (uid, total_comments)
        val usersCommentCount = ctx.broadcast(
            comments.map {
                    case (uid, comment) => (uid, 1L)
                }
                .reduceByKey(_ + _)
                .collectAsMap
        )

        // Tokenise comments ((uid, token), 1)
        val usersTokens = comments.flatMap {
                case (uid, comment) => getTokens(comment).map {
                    case token => ((uid, token), 1L)
                }
            }

        // Get per-user token frequencies
        val userTokenFrequencies = usersTokens.reduceByKey(_ + _)
            .map {
                case ((uid, token), freq) => (uid, (token, freq))
            }

        // Get top X distinctive terms for each user (uid, List[(token, score)])
        val usersDistinctive = userTokenFrequencies.combineByKey (
                // Initialise group's top terms list with first term
                (v) => List[(String, Long)](v),

                // Insert term into this group's top terms list
                (acc: List[(String, Long)], v) => getTopTerms(acc ++ List(v), options.numTerms),

                // Merge top terms lists produced by each partition
                (acc: List[(String, Long)], acc2: List[(String, Long)]) => getTopTerms(acc ++ acc2, options.numTerms)
            )

        // Save to ElasticSearch
        usersDistinctive.map {
                case (uid, distinctive) => Map(
                    "user_id" -> uid,
                    "username" -> users.value(uid),
                    "num_comments" -> usersCommentCount.value(uid),
                    "terms" -> distinctive.map {
                        case (token, score) => Map(
                            "key" -> token,
                            "score" -> score
                        )
                    }
                )
            }
            .saveToEs("%s/%s".format(options.esIndex, options.esContent))

        // Finish
        ctx.stop()

    }

    /**
     * Tokenise a string
     */
    def getTokens(str: String): List[String] = {

        // Split on whitespace
        tokenise.split(str)
            .toList

            // Convert all tokens to lowercase
            .map(_.toLowerCase)

            // Remove leading punctuation (except # and @)
            .map(leadingPunctuation.replaceFirstIn(_, ""))

            // Remove all trailing punctuation
            .map(trailingPunctuation.replaceFirstIn(_, ""))

            // Remove stop words
            .filterNot(stopWords)

            // Remove unwanted tokens
            .filter(
                token => !(
                    // Empty tokens
                    token.isEmpty ||

                    // URLs
                    // TODO: Find a better way to remove URLs
                    token.startsWith("http://") ||
                    token.startsWith("https://")
                )
            )

    }

    /**
     * Sort and trim traversable (token, score) by score value
     */
    def getTopTerms(inputTerms: TraversableOnce[(String, Long)],
                    numTerms: Int)
                    : List[(String, Long)] = {

        var topTerms = List[(String, Long)]()
        var min = Double.MaxValue
        var len = 0

        inputTerms.foreach { e =>
            if (len < numTerms || e._2 > min) {
                topTerms = (e :: topTerms).sortBy((f) => f._2)
                min = topTerms.head._2
                len += 1
            }
            if (len > numTerms) {
                topTerms = topTerms.tail
                min = topTerms.head._2
                len -= 1
            }
        }

        return topTerms

    }

}
