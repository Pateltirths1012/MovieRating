package edu.neu.coe.csye7200.csv.MovieRatingTest

import edu.neu.coe.csye7200.csv.MovieRating
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class MovieRatingTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Unit Tests")
      .master("local")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }


  "calculateMeanRating" should "return the correct mean rating" in {
    val spark = this.spark
    import spark.implicits._

    val testData = Seq(
      (1, "Movie1", 8.0),
      (2, "Movie2", 7.5),
      (3, "Movie3", 6.5),
      (4, "Movie4", 9.0)
    )

    val testDF = testData.toDF("id", "title", "imdb_score")

    val result = MovieRating.calculateMeanRating(testDF)

    result shouldEqual 7.75

  }

  "calculateStdDevRating" should "return the correct standard deviation" in {
    val spark = this.spark
    import spark.implicits._

    val testData = Seq(
      (1, "Movie1", 8.0),
      (2, "Movie2", 7.5),
      (3, "Movie3", 6.5),
      (4, "Movie4", 9.0)
    )

    val testDF = testData.toDF("id", "title", "imdb_score")

    val result = MovieRating.calculateStdDevRating(testDF)

    result shouldEqual 1.0408329997330665
  }

}
