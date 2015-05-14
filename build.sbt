name := "Model Builder"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.0"

/*lazy val root = (project in file(".")).
  settings(
    name := "ModelBuilder",
    version := "1.0",
    scalaVersion := "2.10.4",
    mainClass in Compile := Some("de.haw.bachelorthesis.dkirchner.ModelBuilder")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.3.0" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.3.0" % "1.3.0"
)

// META-INF discarding
assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}*/