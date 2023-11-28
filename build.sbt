ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "MovieRating"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "com.github.tototoshi" %% "scala-csv" % "1.3.10",
  "org.scalatest" %% "scalatest" % "3.2.10" % "test"
)