scalacOptions ++= Seq("-deprecation", "-unchecked")

addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.2.1")

resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.6.2")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.3.0")