ThisBuild / scalaVersion := "3.4.2"
ThisBuild / fork := true
Compile / mainClass := Some("flow.Example")

resolvers ++= Seq(
  "nexus-releases" at "https://nexus.redplanetlabs.com/repository/maven-public-releases",
  "clojars" at "https://repo.clojars.org/"
)

val ceVersion = "3.5.4"
val fs2Version = "3.13.0"

libraryDependencies ++= Seq(
	"com.rpl" % "rama" % "1.9.0" /*% Provided*/,
	"org.typelevel" %% "cats-effect" % ceVersion,
	"co.fs2" %% "fs2-core" % fs2Version,
	"co.fs2" %% "fs2-io" % fs2Version,
	"org.tpolecat" %% "sourcepos" % "1.2.0",
)

val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.5.6"

libraryDependencies ++= (
  Seq("weaver-cats", "weaver-scalacheck").map("com.disneystreaming" %% _ % "0.8.3")
  ++ Seq(
    "net.gfxmonk" %% "flogger-weaver" % "1.0.0",
    logbackClassic,
    "net.gfxmonk" %% "auditspec" % "0.3.1",
    "org.scalacheck" %% "scalacheck" % "1.18.0",
  )
).map(_ % Test)

testFrameworks += new TestFramework("weaver.framework.CatsEffect")
