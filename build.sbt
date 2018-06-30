lazy val `ReactiveMongo-BSON` = Bson.module

lazy val `ReactiveMongo-BSON-Macros` = project.in(file("macros")).
  enablePlugins(CpdPlugin).
  dependsOn(`ReactiveMongo-BSON`).
  settings(
    Common.settings ++ Findbugs.settings ++ Seq(
      libraryDependencies ++= Seq(Dependencies.specs.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % Provided,
        Dependencies.shapelessTest % Test
      )
    )
  )

lazy val `ReactiveMongo-Shaded` = Shaded.commonModule

lazy val `ReactiveMongo-Shaded-Native-osx-x86_64` =
  Shaded.nativeModule("osx-x86_64", "kqueue")

lazy val `ReactiveMongo-Shaded-Native-linux-x86_64` =
  Shaded.nativeModule("linux-x86_64", "epoll")

lazy val `ReactiveMongo` = new Driver(
  `ReactiveMongo-BSON-Macros`,
  `ReactiveMongo-Shaded`,
  `ReactiveMongo-Shaded-Native-linux-x86_64`,
  `ReactiveMongo-Shaded-Native-osx-x86_64`
).module

lazy val `ReactiveMongo-JMX` = new Jmx(`ReactiveMongo`).module

// ---

def docSettings = Documentation(excludes = Seq(`ReactiveMongo-Shaded`, `ReactiveMongo-JMX`)).settings

lazy val `ReactiveMongo-Root` = project.in(file(".")).
  enablePlugins(ScalaUnidocPlugin, CpdPlugin).
  settings(Common.settings ++ docSettings ++
    Travis.settings ++ Seq(
    publishArtifact := false,
    mimaPreviousArtifacts := Set.empty
  )).aggregate(
    `ReactiveMongo-BSON`,
    `ReactiveMongo-BSON-Macros`,
    `ReactiveMongo-Shaded`,
    `ReactiveMongo-Shaded-Native-osx-x86_64`,
    `ReactiveMongo-Shaded-Native-linux-x86_64`,
    `ReactiveMongo`,
    `ReactiveMongo-JMX`)
