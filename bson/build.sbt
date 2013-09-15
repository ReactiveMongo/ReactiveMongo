//scalacOptions ++= Seq("-unchecked", "-deprecation", "-Xlog-implicits", "-Yinfer-debug")

// scalacOptions ++= Seq("-unchecked", "-deprecation", "-Xlog-implicits", "-Yinfer-debug", "-Xprint:typer")

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies += "org.specs2" %% "specs2" % "1.13" % "test"

scalacOptions in (Compile, doc) ++= Opts.doc.title("ReactiveMongo-BSON API")

scalacOptions in (Compile, doc) ++= Opts.doc.version("0.1-SNAPSHOT")
