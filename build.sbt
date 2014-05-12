name := "jdml"

version := "1.0"

scalaVersion := "2.10.3"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "0.9.1"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "0.9.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0"
