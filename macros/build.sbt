organization := "org.reactivemongo"

name := "ReactiveMongo-BSON-Macros"

version := "0.9-SNAPSHOT"

scalaVersion := "2.10.0"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies += "org.specs2" %% "specs2" % "1.13" % "test"

scalacOptions in (Compile, doc) ++= Opts.doc.title("ReactiveMongo-BSON-Macros API")

scalacOptions in (Compile, doc) ++= Opts.doc.version("0.9-SNAPSHOT")
