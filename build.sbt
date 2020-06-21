lazy val `ReactiveMongo-Core` = project.in(file("core")).
  settings(
    sourceDirectories in Compile ++= Seq(
      (sourceDirectory in Compile).value / "scala-2.11+"),
    libraryDependencies ++= {
      val deps = Dependencies.shaded.value

      ("org.reactivemongo" %% "reactivemongo-bson-api" % version.
        value) +: deps
    }
  )

lazy val `ReactiveMongo` = new Driver(`ReactiveMongo-Core`).module

lazy val `ReactiveMongo-Test` = project.in(file("test")).settings(
  description := "ReactiveMongo test helpers",
).dependsOn(`ReactiveMongo`)

// ---

def docSettings = Documentation(excludes = Seq.empty).settings

lazy val `ReactiveMongo-Root` = project.in(file(".")).
  enablePlugins(ScalaUnidocPlugin).
  settings(docSettings ++ Seq(
    publishArtifact := false,
    publishTo := None,
    publishLocal := {},
    publish := {}
  )).aggregate(
    `ReactiveMongo-Core`,
    `ReactiveMongo`,
    `ReactiveMongo-Test`)

lazy val benchmarks = (project in file("benchmarks")).
  enablePlugins(JmhPlugin).
  settings(Compiler.settings ++ Seq(
      libraryDependencies += organization.value % "reactivemongo-shaded" % version.value
    )
  ).
  dependsOn(`ReactiveMongo`)
