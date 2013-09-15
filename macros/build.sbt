scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies += "org.specs2" %% "specs2" % "1.13" % "test"

scalacOptions in (Compile, doc) ++= Opts.doc.title("ReactiveMongo-BSON-Macros API")

scalacOptions in (Compile, doc) ++= Opts.doc.version("0.9-SNAPSHOT")
