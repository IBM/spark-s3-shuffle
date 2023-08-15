//
// Copyright 2022- IBM Inc. All rights reserved
// SPDX-License-Identifier: Apache2.0
//

scalaVersion := sys.env.getOrElse("SCALA_VERSION", "2.12.15")
organization := "com.ibm"
name := "spark-s3-shuffle"

val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "3.1.2")

enablePlugins(GitVersioning, BuildInfoPlugin)

// Git
git.useGitDescribe := true
git.uncommittedSignifier := Some("DIRTY")

// Build info
buildInfoObject := "SparkS3ShuffleBuild"
buildInfoPackage := "com.ibm"
buildInfoKeys ++= Seq[BuildInfoKey](
  BuildInfoKey.action("buildTime") {
    System.currentTimeMillis
  },
  BuildInfoKey.action("sparkVersion") {
    sparkVersion
  }
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // Note: The tests don't work with Scala 2.13
  "junit" % "junit" % "4.13.2" % Test, // TRAVIS_SCALA_WORKAROUND_REMOVE_LINE
  "org.scalatest" %% "scalatest" % "3.2.2" % Test, // TRAVIS_SCALA_WORKAROUND_REMOVE_LINE
  "ch.cern.sparkmeasure" %% "spark-measure" % "0.18" % Test, // TRAVIS_SCALA_WORKAROUND_REMOVE_LINE
  "org.scalacheck" %% "scalacheck" % "1.15.2" % Test, // TRAVIS_SCALA_WORKAROUND_REMOVE_LINE
  "org.mockito" % "mockito-core" % "3.4.6" % Test, // TRAVIS_SCALA_WORKAROUND_REMOVE_LINE
  "org.scalatestplus" %% "mockito-3-4" % "3.2.9.0" % Test // TRAVIS_SCALA_WORKAROUND_REMOVE_LINE
  )
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
scalacOptions ++= Seq("-deprecation", "-unchecked")

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}

assemblyMergeStrategy := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion}_${version}-with-dependencies.jar"
assembly / assemblyOption ~= {
  _.withIncludeScala(false)
}

lazy val lib = (project in file("."))
