scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature")

resolvers ++= Seq(
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

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.3")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.2")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.6.1")

addSbtPlugin("com.github.sbt" % "sbt-findbugs" % "2.0.0")

addSbtPlugin("com.github.sbt" % "sbt-cpd" % "2.0.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.12")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.3.7")
