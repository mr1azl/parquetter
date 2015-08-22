import AssemblyKeys._
import sbtassembly.Plugin.MergeStrategy

assemblySettings

name := "Parquetter"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10"  % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-catalyst_2.10" % "1.4.1" % "provided"

libraryDependencies += "com.twitter.elephantbird" % "elephant-bird" % "4.5"

libraryDependencies += "com.twitter.elephantbird" % "elephant-bird-core" % "4.5"

libraryDependencies += "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.17"

libraryDependencies += "com.google.code.gson" % "gson" % "2.3.1"

libraryDependencies += "com.spatial4j" % "spatial4j" % "0.4.1"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "3.0.0-M7" % "test"

libraryDependencies += "junit" % "junit" % "4.12" % "test"



mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
  case "about.html"  => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
}



resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)




