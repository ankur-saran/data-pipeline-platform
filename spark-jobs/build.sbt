// =============================================================================
// data-pipeline-platform — sbt build definition
// =============================================================================

ThisBuild / version      := "1.0.0"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "com.pipeline"

val sparkVersion = "3.4.2"

lazy val root = (project in file("."))
  .settings(
    name := "data-pipeline-platform",

    libraryDependencies ++= Seq(
      // Spark — provided at runtime
      "org.apache.spark" %% "spark-core"      % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"        % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive"       % sparkVersion % "provided",

      // Config
      "com.typesafe"      % "config"           % "1.4.3",

      // Logging
      "org.apache.logging.log4j" % "log4j-api"        % "2.20.0",
      "org.apache.logging.log4j" % "log4j-core"       % "2.20.0",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0",

      // CLI arg parsing
      "com.github.scopt" %% "scopt" % "4.1.0",

      // Test — include Spark in test scope
      "org.scalatest"    %% "scalatest"        % "3.2.17"     % Test,
      "org.apache.spark" %% "spark-core"       % sparkVersion % Test,
      "org.apache.spark" %% "spark-sql"        % sparkVersion % Test
    ),

    // Do not run tests in parallel — Spark SparkContext conflicts
    Test / parallelExecution := false,
    Test / fork              := false,

    // Fat-jar assembly settings
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*)             => MergeStrategy.discard
      case "reference.conf"                           => MergeStrategy.concat
      case "application.conf"                         => MergeStrategy.discard
      case _                                          => MergeStrategy.first
    },

    // Scala compiler options
    scalacOptions ++= Seq(
      "-encoding", "utf8",
      "-deprecation",
      "-unchecked",
      "-feature",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen"
    )
  )
