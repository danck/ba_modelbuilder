lazy val root = (project in file(".")).
  settings(
    name := "model-builder",
    version := "1.0",
    scalaVersion := "2.10.4",
    mainClass in Compile := Some("de.haw.bachelorthesis.dkirchner.ModelBuilder"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
      "org.apache.spark" % "spark-mllib_2.10" % "1.3.0",
      "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
      "javax.mail" % "javax.mail-api" % "1.4.7",
      "javax.mail" % "mail" % "1.4.7"
    )
)

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}