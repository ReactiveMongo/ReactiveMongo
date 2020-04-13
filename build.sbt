lazy val `ReactiveMongo-BSON` = new Bson().module

lazy val `ReactiveMongo-BSON-Macros` = project.in(file("macros")).
  dependsOn(`ReactiveMongo-BSON`).
  settings(Findbugs.settings ++ Seq(
    mimaBinaryIssueFilters += {
      import com.typesafe.tools.mima.core._, ProblemFilters.{ exclude => x }

      x[MissingTypesProblem]("reactivemongo.bson.Macros$Options$UnionType")
    },
    libraryDependencies ++= Seq(Dependencies.specs.value,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value % Provided,
      Dependencies.shapelessTest % Test
    )
  ))

lazy val `ReactiveMongo-BSON-Compat` = project.in(file("bson-compat")).
  settings(Seq(
    //name := s"${baseArtifact}-compat",
    description := "Compatibility library between legacy & new BSON APIs",
    sourceDirectory := {
      if (scalaBinaryVersion.value == "2.10") new java.io.File("/no/sources")
      else sourceDirectory.value
    },
    publishArtifact := (scalaBinaryVersion.value != "2.10"),
    publish := (Def.taskDyn {
      val ver = scalaBinaryVersion.value
      val go = publish.value

      Def.task {
        if (ver != "2.13") {
          go
        }
      }
    }).value,
    fork in Test := true,
    libraryDependencies ++= {
      if (scalaBinaryVersion.value != "2.10") {
        Dependencies.shaded.value ++ Seq(
          organization.value %% "reactivemongo-bson-api" % version.value % Provided,
          Dependencies.specs.value)

      } else {
        Seq.empty[ModuleID]
      }
    }
  )).dependsOn(`ReactiveMongo-BSON`)

lazy val `ReactiveMongo-Core` = project.in(file("core")).
  dependsOn(`ReactiveMongo-BSON` % Provided).
  settings(
    Findbugs.settings ++ Seq(
      sourceDirectories in Compile ++= {
        if (scalaBinaryVersion.value != "2.10") {
          Seq((sourceDirectory in Compile).value / "scala-2.11+")
        } else {
          Seq.empty
        }
      },
      libraryDependencies ++= {
        val deps = Dependencies.shaded.value

        if (scalaBinaryVersion.value != "2.10") {
          ("org.reactivemongo" %% "reactivemongo-bson-api" % version.
            value) +: deps
        } else {
          deps
        }
      }
    ))

lazy val `ReactiveMongo` = new Driver(
  `ReactiveMongo-BSON`,
  `ReactiveMongo-BSON-Macros`,
  `ReactiveMongo-Core`,
  `ReactiveMongo-BSON-Compat`
).module

// ---

def docSettings = Documentation(excludes = Seq.empty).settings

lazy val `ReactiveMongo-Root` = project.in(file(".")).
  enablePlugins(ScalaUnidocPlugin).
  settings(docSettings ++
    Travis.settings ++ Seq(
      publishArtifact := false,
      publishTo := None,
      publishLocal := {},
      publish := {}
  )).aggregate(
    `ReactiveMongo-BSON`,
    `ReactiveMongo-BSON-Macros`,
    `ReactiveMongo-BSON-Compat`,
    `ReactiveMongo-Core`,
    `ReactiveMongo`)

lazy val benchmarks = (project in file("benchmarks")).
  enablePlugins(JmhPlugin).
  settings(Compiler.settings ++ Seq(
      libraryDependencies += organization.value % "reactivemongo-shaded" % version.value
    )
  ).
  dependsOn(
    `ReactiveMongo-BSON` % "compile->test",
    `ReactiveMongo`)
