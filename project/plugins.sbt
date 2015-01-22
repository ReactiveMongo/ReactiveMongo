scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature")

addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.3.0")

resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.6.4")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.3.1")
