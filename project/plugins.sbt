scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature")

resolvers ++= Seq(
  "jgit-repo" at "http://download.eclipse.org/jgit/maven",
  Resolver.url("sbt-repo", url(
    "https://dl.bintray.com/sbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns),
  Resolver.url("typesafe-repo", url(
   "https://dl.bintray.com/typesafe/sbt-plugins/"))(Resolver.ivyStylePatterns),
  Resolver.url("eed3si9n-repo", url(
    "https://dl.bintray.com/eed3si9n/sbt-plugins/"))(Resolver.ivyStylePatterns),
  Resolver.url("typesafe-repo", url(
   "https://dl.bintray.com/typesafe/sbt-plugins/"))(Resolver.ivyStylePatterns),
  Resolver.url("jsuereth-repo", url(
    "https://dl.bintray.com/jsuereth/sbt-plugins/"))(Resolver.ivyStylePatterns))

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.7.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.8.4")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.3.3-dcdc4774d19d1500437bc63e79c3abb8f99bcdb4")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.1")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.1.8")

addSbtPlugin("de.johoop" % "findbugs4sbt" % "1.4.0")

addSbtPlugin("de.johoop" % "cpd4sbt" % "1.2.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.3")
