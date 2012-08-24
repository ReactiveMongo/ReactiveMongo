name := "ReactiveMongo"

organization := "reactivemongo"

version := "0.1-SNAPSHOT"


resolvers += "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"

resolvers += "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/" 

resolvers += Resolver.file("local repository", file("/Users/sgo/.ivy2/local"))(Resolver.ivyStylePatterns)

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.3.1.Final",
  "com.typesafe.akka" % "akka-actor" % "2.0",
  "play" %% "play" % "2.1-SNAPSHOT",
  "ch.qos.logback" % "logback-core" % "1.0.0",
  "ch.qos.logback" % "logback-classic" % "1.0.0",
  "org.specs2" % "specs2_2.9.1" % "1.7.1" % "test",
  "junit" % "junit" % "4.8" % "test"
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Ydependent-method-types")

unmanagedSourceDirectories in Compile <+= baseDirectory( _ / "src" / "samples" / "scala" )

publishTo <<= version { (version: String) =>
  val localPublishRepo = "/Volumes/Data/code/repository"
  if(version.trim.endsWith("SNAPSHOT"))
    Some(Resolver.file("snapshots", new File(localPublishRepo + "/snapshots")))
  else Some(Resolver.file("releases", new File(localPublishRepo + "/releases")))
}

publishMavenStyle := true

sources in (Compile, doc) ~= (_ filter (!_.getAbsolutePath.contains("src/samples")))

scalaVersion := "2.9.2"
