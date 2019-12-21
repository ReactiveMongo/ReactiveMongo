lazy val `ReactiveMongo-BSON` = new Bson().module

lazy val `ReactiveMongo-BSON-Macros` = project.in(file("macros")).
  enablePlugins(CpdPlugin).
  dependsOn(`ReactiveMongo-BSON`).
  settings(
    Common.settings ++ Findbugs.settings ++ Seq(
      mimaBinaryIssueFilters += {
        import com.typesafe.tools.mima.core._, ProblemFilters.{ exclude => x }

        x[MissingTypesProblem]("reactivemongo.bson.Macros$Options$UnionType")
      },
      libraryDependencies ++= Seq(Dependencies.specs.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % Provided,
        Dependencies.shapelessTest % Test
      )
    )
  )

lazy val `ReactiveMongo-BSON-Compat` = project.in(file("bson-compat")).
  settings(Common.settings ++ Seq(
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
    mimaPreviousArtifacts := Set.empty,
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
  enablePlugins(CpdPlugin).
  dependsOn(`ReactiveMongo-BSON` % Provided).
  settings(
    Common.settings ++ Findbugs.settings ++ Seq(
      mimaPreviousArtifacts := {
        val v = scalaBinaryVersion.value
        import Publish.previousVersion

        if (v == "2.12" && crossPaths.value) {
          Set(organization.value % s"reactivemongo_${scalaBinaryVersion.value}" % "0.12.7")
        } else if (v == "2.13") {
          Set.empty
        } else if (crossPaths.value) {
          Set(organization.value % s"reactivemongo_${scalaBinaryVersion.value}" % previousVersion)
        } else {
          Set(organization.value % "reactivemongo" % previousVersion)
        }
      },
      mimaBinaryIssueFilters ++= {
        import com.typesafe.tools.mima.core._, ProblemFilters.{ exclude => x }

        @inline def fcp(s: String) = x[FinalClassProblem](s)
        @inline def mtp(s: String) = x[MissingTypesProblem](s)
        @inline def isp(s: String) = x[IncompatibleSignatureProblem](s)

        Seq(
          mtp("reactivemongo.core.protocol.ResponseDecoder"),
          mtp("reactivemongo.core.protocol.ResponseInfo$"),
          fcp("reactivemongo.core.protocol.ResponseInfo"),
          isp("reactivemongo.core.protocol.KillCursors.writeTo"),
          isp("reactivemongo.core.protocol.Query.writeTo"),
          isp("reactivemongo.core.protocol.GetMore.writeTo"),
          isp("reactivemongo.core.protocol.Insert.writeTo"),
          isp("reactivemongo.core.protocol.Update.writeTo"),
          isp("reactivemongo.core.protocol.MessageHeader.writeTo"),
          isp("reactivemongo.core.protocol.ChannelBufferWritable.writeTo"),
          isp("reactivemongo.core.protocol.Delete.writeTo"),
          isp("reactivemongo.core.protocol.Response.unapply"),
          isp("reactivemongo.core.protocol.ResponseInfo.andThen"),
          isp("reactivemongo.core.protocol.ResponseInfo.compose"),
          isp("reactivemongo.api.BSONSerializationPack.readAndDeserialize")
        )
      },
      //mimaPreviousArtifacts := Set.empty,
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
  settings(Common.settings ++ Compiler.settings ++ Seq(
      libraryDependencies += organization.value % "reactivemongo-shaded" % version.value
    )
  ).
  dependsOn(
    `ReactiveMongo-BSON` % "compile->test",
    `ReactiveMongo`)
