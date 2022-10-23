ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.13.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14" % Test


lazy val root = (project in file("."))
  .settings(
    name := "Challenge"
  )
