lazy val `ReactiveMongo-BSON` = Bson.module

lazy val `ReactiveMongo-BSON-Macros` = project.in(file("macros")).
  enablePlugins(CpdPlugin).
  dependsOn(`ReactiveMongo-BSON`).
  settings(
    BuildSettings.settings ++ Findbugs.settings ++ Seq(
      libraryDependencies ++= Seq(Dependencies.specs.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % Provided,
        Dependencies.shapelessTest % Test
      )
    )
  )

lazy val `ReactiveMongo-Shaded` = Shaded.module

lazy val `ReactiveMongo` = new Driver(
  `ReactiveMongo-BSON-Macros`,
  `ReactiveMongo-Shaded`).module

lazy val `ReactiveMongo-JMX` = new Jmx(`ReactiveMongo`).module

// ---

def docSettings = Documentation(excludes = Seq(`ReactiveMongo-Shaded`, `ReactiveMongo-JMX`)).settings

lazy val `ReactiveMongo-Root` = project.in(file(".")).
  enablePlugins(ScalaUnidocPlugin, CpdPlugin).
  settings(BuildSettings.settings ++ docSettings ++
    Travis.settings ++ Seq(
    publishArtifact := false,
    mimaPreviousArtifacts := Set.empty
  )).aggregate(
    `ReactiveMongo-BSON`,
    `ReactiveMongo-BSON-Macros`,
    `ReactiveMongo-Shaded`,
    `ReactiveMongo`,
    `ReactiveMongo-JMX`)
