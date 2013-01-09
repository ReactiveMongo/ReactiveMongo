name := "ReactiveMongo"

organization := "reactivemongo"

version := "0.1-SNAPSHOT"

resolvers += Resolver.file("LocalPlayRepo", file("/Volumes/Data/zenexity/Play20/repository/local"))(Resolver.ivyStylePatterns)

resolvers += "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"

resolvers += "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/" 

// resolvers += Resolver.file("local repository", file("/Users/sgo/.ivy2/local"))(Resolver.ivyStylePatterns)

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.3.1.Final",
  "com.typesafe.akka" % "akka-actor_2.10.0-RC2" % "2.1.0-RC2",
  "play" % "play-iteratees_2.10" % "2.1-SNAPSHOT",
  "ch.qos.logback" % "logback-core" % "1.0.0",
  "ch.qos.logback" % "logback-classic" % "1.0.0",
  "org.specs2" % "specs2_2.10.0-RC2" % "1.12.2" % "test",
  "junit" % "junit" % "4.8" % "test"
)

scalacOptions ++= Seq("-unchecked", "-deprecation")

unmanagedSourceDirectories in Compile <+= baseDirectory( _ / "src" / "samples" / "scala" )

publishTo <<= version { (version: String) =>
  val localPublishRepo = "/Volumes/Data/code/repository"
  if(version.trim.endsWith("SNAPSHOT"))
    Some(Resolver.file("snapshots", new File(localPublishRepo + "/snapshots")))
  else Some(Resolver.file("releases", new File(localPublishRepo + "/releases")))
}

mappings in (Compile,packageBin) ~= { (ms: Seq[(File, String)]) =>
  ms filter { case (file, toPath) =>
    val b = toPath != "logback.xml" && !toPath.startsWith("foo") && !toPath.startsWith("tests") && !toPath.startsWith("yop")
    println("path is " + toPath)
    b
  }
}

publishMavenStyle := true

sources in (Compile, doc) ~= (_ filter (p => !p.getAbsolutePath.contains("src/samples") && !p.getAbsolutePath.contains("src/main/scala/tests")))

scalaVersion := "2.10.0"
