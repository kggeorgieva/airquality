val sparkVersion = "2.3.1"
val scalaTestVersion = "3.0.5"
name := "airquality"
version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++=  Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" excludeAll(ExclusionRule(name = "slf4j-log4j12"), ExclusionRule(name = "log4j")),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" excludeAll(ExclusionRule(name = "slf4j-log4j12"), ExclusionRule(name = "log4j")),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile, test" classifier "tests" excludeAll(ExclusionRule(name = "slf4j-log4j12"), ExclusionRule(name = "log4j")),

  "com.github.pureconfig" %% "pureconfig" % "0.9.1",

  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",

  "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3" excludeAll(ExclusionRule(name = "slf4j-log4j12"),
    ExclusionRule(name = "log4j"))
)