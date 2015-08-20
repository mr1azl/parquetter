name := "Parquetter"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"

libraryDependencies +=  "org.scalatest" % "scalatest_2.10.0-M4" % "1.9-2.10.0-M4-B1"

libraryDependencies +=  "junit" % "junit" % "4.8.1" % "test"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.4.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1"

libraryDependencies += "com.spatial4j" %% "spatial4j" % "0.4.1"

libraryDependencies += "com.google.code.gson" %% "gson" % "2.3.1"


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
