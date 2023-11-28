package edu.neu.coe.csye7200.csv

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object MovieRating {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Movie Rating")
      .master("local")
      .getOrCreate()

    //Reading the CSV file into a DataFrame
    val movieDataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\patel\\IdeaProjects\\MovieRating\\src\\main\\resources\\movie_metadata.csv")



    //processing the data
    val processedData = processData(movieDataFrame)

    //calculating mean and standard deviation for all movies
    val meanRating = calculateMeanRating(processedData)
    val stdDevRating = calculateStdDevRating(processedData)

    // Display results
    println(s"Mean Rating of all movies: $meanRating")
    println(s"Standard Deviation of all movies: $stdDevRating")

    spark.stop()
  }

  //function to process the data
  def processData(movieDataFrame: DataFrame): DataFrame = {
    //Dropping rows with missing values
    val cleanedData = movieDataFrame.na.drop()

    cleanedData
  }

  //method to calculate mean rating
  def calculateMeanRating(df: DataFrame): Double = {
    df.agg(mean("imdb_score").as("mean")).first().getDouble(0)
  }

  //method to calculate standard deviation of ratings
  def calculateStdDevRating(df: DataFrame): Double = {
    df.agg(stddev("imdb_score").as("stddev")).first().getDouble(0)
    }
}