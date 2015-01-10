name := "Stoop"

version := "0.9.10-SNAPSHOT"

scalaVersion := "2.10.3"

scalacOptions += "-deprecation"

scalacOptions += "-feature"

resolvers += "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

resolvers += "spray repo" at "http://repo.spray.io"

libraryDependencies ++= Seq("org.scalaz" %% "scalaz-core" % "7.1.0",
                            "io.spray" %% "spray-json" % "1.2.5",
                            "org.scalaz" %% "scalaz-effect" % "7.1.0",
                            "org.scalaz" %% "scalaz-concurrent" % "7.1.0",
                            "org.scalaz.stream" %% "scalaz-stream" % "0.1")

libraryDependencies += "net.databinder.dispatch" %% "dispatch-core" % "0.10.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.10.1" % "test"

publishMavenStyle := true
