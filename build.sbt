//
// Copyright 2022- IBM Inc. All rights reserved
// SPDX-License-Identifier: Apache2.0
//

scalaVersion := sys.env.getOrElse("SCALA_VERSION", "2.12.18")
organization := "com.ibm"
name := "spark-s3-shuffle"
val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "3.5.5")

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
  "org.apache.spark" %% "spark-hadoop-cloud" % sparkVersion % "compile",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
scalacOptions ++= Seq("-deprecation", "-unchecked")

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}

assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion}_${version}-with-dependencies.jar"
assembly / assemblyOption ~= {
  _.withIncludeScala(false)
}

lazy val lib = (project in file("."))
