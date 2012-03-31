name := "Mongo Async Driver"

organization := "org.asyncmongo"

version := "0.1-SNAPSHOT"


resolvers += "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"

resolvers += "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/" 

resolvers += Resolver.file("local repository", file("/Users/pvo/.ivy2/local"))(Resolver.ivyStylePatterns)

libraryDependencies ++= Seq(
	"io.netty" % "netty" % "3.3.1.Final",
	"de.undercouch" % "bson4jackson" % "1.2.0",
	"com.typesafe.akka" % "akka-actor" % "2.0"
)
