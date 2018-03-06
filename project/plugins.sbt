logLevel := Level.Warn

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")

addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.4.0")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.2")

addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.3.4")

addSbtPlugin("au.com.onegeek" %% "sbt-dotenv" % "1.2.88")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.17")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.7.0"
