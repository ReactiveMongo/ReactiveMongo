organization := "org.reactivemongo"

name := "ReactiveMongo-DSL"

version := "0.9"

scalaVersion := "2.10.1"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies += "org.specs2" %% "specs2" % "1.13" % "test"

scalacOptions in (Compile, doc) ++= Opts.doc.title("ReactiveMongo-DSL API")

scalacOptions in (Compile, doc) ++= Opts.doc.version("0.9-SNAPSHOT")
