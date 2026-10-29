import org.typelevel.scalacoptions.ScalacOptions
import org.typelevel.sbt.tpolecat.{CiMode, DevMode, ReleaseMode, VerboseMode}

ThisBuild / tpolecatDefaultOptionsMode := DevMode

ThisBuild / scalaVersion := "3.4.2"
ThisBuild / fork := true

ThisBuild / resolvers ++= Seq(
  "nexus-releases" at "https://nexus.redplanetlabs.com/repository/maven-public-releases",
  "clojars" at "https://repo.clojars.org/"
)

val ceVersion = "3.5.4"

val fs2Version = "3.13.0"

val floggerVersion = "1.1.0"

val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.5.6"

val commonSettings = Seq(
  testFrameworks += new TestFramework("weaver.framework.CatsEffect"),
  libraryDependencies ++= (
    Seq(
      "net.gfxmonk" %% "flogger-cats" % floggerVersion,
    )
    ++ Seq("circe-core", "circe-generic", "circe-parser").map("io.circe" %% _ % "0.14.1")

    // test
    ++ (
      Seq("weaver-cats", "weaver-scalacheck").map("org.typelevel" %% _ % "0.13.0")
      ++ Seq(
        "net.gfxmonk" %% "flogger-weaver" % floggerVersion,
        logbackClassic,
        "net.gfxmonk" %% "auditspec" % "0.3.1",
        // "org.scalacheck" %% "scalacheck" % "1.18.0",
      )
    ).map(_ % Test)
  )
)

lazy val pb = (project in file("protobuf"))
  .settings(
    name := "corama-pb",
    scalacOptions += "-nowarn", // protobuf compiler generates a bunch of warnings
    scalapbCodeGeneratorOptions += CodeGeneratorOption.Fs2Grpc,
  ).enablePlugins(
    Fs2Grpc
  )

lazy val core = (project in file("core")).settings(
  name := "corama-core",
  commonSettings,
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % ceVersion,
    "co.fs2" %% "fs2-core" % fs2Version,
    "co.fs2" %% "fs2-io" % fs2Version,
    "org.tpolecat" %% "sourcepos" % "1.2.0",
    "dev.zio" %% "izumi-reflect" % "3.0.9",
  ),
).dependsOn(pb)

lazy val interpreter = (project in file("interpreter")).settings(
  name := "corama-interpreter",
  commonSettings,
  libraryDependencies ++= Seq(
    "org.rocksdb" % "rocksdbjni" % "9.3.1",
    "org.typelevel" %% "cats-effect-testkit" % ceVersion % Test,

  ) ++ List("ratis-grpc", "ratis-server").map { component =>
    "org.apache.ratis" % component % "3.3.1"
  },
).dependsOn(core, pb)

lazy val rama = (project in file("rama")).settings(
  name := "corama-rama",
  commonSettings,
  libraryDependencies ++= Seq(
    "com.rpl" % "rama" % "1.9.0" /*% Provided*/,
  ),
  
  // grpc depends on *-android version, but rama requires *-jre
  // (symptoms of mismatch: failure to load com.google.common.io.MoreFiles)
  dependencyOverrides += "com.google.guava" % "guava" % "32.1.3-jre",
).dependsOn(core)

lazy val example = (project in file("example")).settings(
  commonSettings,
  Compile / mainClass := Some("example.Example"),
  dependencyOverrides += "com.google.guava" % "guava" % "32.1.3-jre",
).dependsOn(core, rama, interpreter)

lazy val root = (project in file(".")).settings(
  name := "root",
).aggregate(core, interpreter, rama, example)
