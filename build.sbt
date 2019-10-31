lazy val `ReactiveMongo-BSON` = new Bson().module

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

lazy val `ReactiveMongo-BSON-Compat` = project.in(file("bson-compat")).
  settings(Common.settings ++ Seq(
    //name := s"${baseArtifact}-compat",
    crossScalaVersions ~= {
      _.filterNot(_ startsWith "2.10")
    },
    description := "Compatibility library between legacy & new BSON APIs",
    fork in Test := true,
    libraryDependencies ++= Seq(
      Dependencies.shaded.value % Provided,
      organization.value %% "reactivemongo-bson-api" % version.value % Provided,
      Dependencies.specs.value),
  )).dependsOn(`ReactiveMongo-BSON`)

lazy val `ReactiveMongo-Core` = project.in(file("core")).
  enablePlugins(CpdPlugin).
  dependsOn(`ReactiveMongo-BSON` % Provided).
  settings(
    Common.settings ++ Findbugs.settings ++ Seq(
      libraryDependencies += Dependencies.shaded.value,
    ))

lazy val `ReactiveMongo` = new Driver(
  `ReactiveMongo-BSON`,
  `ReactiveMongo-BSON-Macros`,
  `ReactiveMongo-Core`
).module

lazy val `ReactiveMongo-JMX` = new Jmx(`ReactiveMongo`).module

// ---

def docSettings = Documentation(excludes = Seq(`ReactiveMongo-JMX`)).settings

lazy val `ReactiveMongo-Root` = project.in(file(".")).
  enablePlugins(ScalaUnidocPlugin, CpdPlugin).
  settings(Common.settings ++ docSettings ++
    Travis.settings ++ Seq(
      publishArtifact := false,
      publishTo := None,
      publishLocal := {},
      publish := {},
      mimaPreviousArtifacts := Set.empty
  )).aggregate(
    `ReactiveMongo-BSON`,
    `ReactiveMongo-BSON-Macros`,
    `ReactiveMongo-BSON-Compat`,
    `ReactiveMongo-Core`,
    `ReactiveMongo`,
    `ReactiveMongo-JMX`)

lazy val benchmarks = (project in file("benchmarks")).
  enablePlugins(JmhPlugin).
  settings(Common.settings ++ Compiler.settings).
  dependsOn(`ReactiveMongo-BSON` % "compile->test")
