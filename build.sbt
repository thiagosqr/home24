name := "home24"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies := Seq(
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)
