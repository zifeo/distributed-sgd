import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import sbt.Package.ManifestAttributes
import sbtassembly.PathList

name := "Distributed-sgd"
version := "0.1.0"

scalaVersion := "2.12.4"
scalacOptions ++= List(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-explaintypes",
  "-feature",
  "-unchecked",
  "-Xcheckinit",
  "-Xfatal-warnings",
  "-Xlint:adapted-args",
  "-Xlint:by-name-right-associative",
  "-Xlint:constant",
  "-Xlint:delayedinit-select",
  "-Xlint:doc-detached",
  "-Xlint:inaccessible",
  "-Xlint:infer-any",
  "-Xlint:missing-interpolator",
  "-Xlint:nullary-override",
  "-Xlint:nullary-unit",
  "-Xlint:option-implicit",
  "-Xlint:package-object-classes",
  "-Xlint:poly-implicit-overload",
  "-Xlint:private-shadow",
  "-Xlint:stars-align",
  "-Xlint:type-parameter-shadow",
  "-Xlint:unsound-match",
  "-Yno-adapted-args",
  "-Ypartial-unification",
  "-Ywarn-dead-code",
  "-Ywarn-extra-implicit",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Ywarn-unused:locals",
  "-Ywarn-unused:privates",
  "-Ywarn-unused:implicits"
  //"-Ywarn-unused-import"
  //"-Ywarn-value-discard"
)

scalacOptions in (Compile, console) --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings")

scalacOptions in Test --= Seq("-Ywarn-value-discard", "-Ywarn-unused:privates")

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies ++= List(
  "ch.qos.logback"             % "logback-classic"       % "1.2.3",
  "org.apache.logging.log4j"   % "log4j-to-slf4j"        % "2.11.0",
  "com.typesafe"               % "config"                % "1.3.3",
  "io.grpc"                    % "grpc-netty"            % scalapb.compiler.Version.grpcJavaVersion,
  "com.thesamet.scalapb"       %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
  "com.iheart"                 %% "ficus"                % "1.4.3",
  "org.typelevel"              %% "spire"                % "0.15.0",
  "com.typesafe.scala-logging" %% "scala-logging"        % "3.8.0",
  "com.github.pathikrit"       %% "better-files"         % "3.4.0",
  "org.scalatest"              %% "scalatest"            % "3.0.5" % "test"
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

fork := true
cancelable in Global := true

parallelExecution in Test := false
testOptions in Test += Tests.Argument("-oD")

test in assembly := {}
target in assembly := file("build")
assemblyJarName in assembly := "distributed-sgd.jar"
mainClass in assembly := Some("epfl.distributed.Main")
packageOptions := List(
  ManifestAttributes(
    "Change"     -> version.value,
    "Build-Date" -> LocalDateTime.now.format(DateTimeFormatter.ISO_DATE_TIME)
  )
)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "log4j-provider.properties") => MergeStrategy.first
  case x                                                 => (assemblyMergeStrategy in assembly).value(x)
}
