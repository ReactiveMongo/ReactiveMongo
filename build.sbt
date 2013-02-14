scalaVersion := "2.10.0"

// scalacOptions ++= Seq("-Xlog-implicits")

scalacOptions in (Compile, doc) ++= Opts.doc.title("ReactiveMongo API")

scalacOptions in (Compile, doc) ++= Opts.doc.version("0.9-SNAPSHOT")