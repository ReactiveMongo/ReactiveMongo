lazy val `ReactiveMongo-Core` = project
  .in(file("core"))
  .settings(
    Compile / sourceDirectories ++= Seq(
      (Compile / sourceDirectory).value / "scala-2.11+"
    ),
    libraryDependencies ++= {
      val deps = Dependencies.shaded.value

      ("org.reactivemongo" %% "reactivemongo-bson-api" % version.value)
        .exclude("org.slf4j", "*") +: deps ++: Seq(
        "com.github.luben" % "zstd-jni" % "1.5.5-5",
        "org.xerial.snappy" % "snappy-java" % "1.1.10.1",
        Dependencies.specs.value
      )
    },
    // Silent mock for Scala3
    Test / doc / scalacOptions ++= List("-skip-packages", "com.github.ghik"),
    Compile / packageBin / mappings ~= {
      _.filter { case (_, path) => !path.startsWith("com/github/ghik") }
    },
    Compile / packageSrc / mappings ~= {
      _.filter { case (_, path) => path != "silent.scala" }
    },
    //
    mimaBinaryIssueFilters ++= {
      import com.typesafe.tools.mima.core._

      val mtp = ProblemFilters.exclude[MissingTypesProblem](_)

      Seq(
        mtp("reactivemongo.core.protocol.KillCursors"),
        mtp("reactivemongo.core.protocol.Update"),
        mtp("reactivemongo.core.protocol.WriteRequestOp"),
        mtp("reactivemongo.core.protocol.CollectionAwareRequestOp"),
        mtp("reactivemongo.core.protocol.Query"),
        mtp("reactivemongo.core.protocol.MessageHeader$"),
        mtp("reactivemongo.core.protocol.MessageHeader"),
        mtp("reactivemongo.core.protocol.Delete"),
        mtp("reactivemongo.core.protocol.GetMore"),
        mtp("reactivemongo.core.protocol.RequestOp"),
        mtp("reactivemongo.core.protocol.Insert"),
        mtp("reactivemongo.core.protocol.Reply$"),
        mtp("reactivemongo.core.protocol.ResponseDecoder"),
        mtp("reactivemongo.core.actors.MongoDBSystem$OperationHandler"),
        mtp("reactivemongo.core.netty.ChannelFactory"),
        mtp("reactivemongo.core.protocol.MongoHandler"),
        mtp("reactivemongo.core.protocol.ResponseFrameDecoder"),
        mtp("reactivemongo.core.protocol.RequestEncoder")
      )
    }
  )


lazy val `ReactiveMongo-Actors-Akka` = project.in(file("actors-akka")).settings(
  libraryDependencies ++= Dependencies.akka.value
)

lazy val `ReactiveMongo-Actors-Pekko` = project.in(file("actors-pekko")).settings(
  libraryDependencies ++= Dependencies.pekko.value
)

lazy val actorModule = Common.actorModule match {
  case "pekko" => `ReactiveMongo-Actors-Pekko`
  case "akka" => `ReactiveMongo-Actors-Akka`
}

lazy val `ReactiveMongo` = new Driver(`ReactiveMongo-Core`, actorModule).module

lazy val `ReactiveMongo-Test` = project
  .in(file("test"))
  .settings(
    description := "ReactiveMongo test helpers"
  )
  .dependsOn(`ReactiveMongo`)

// ---

lazy val `ReactiveMongo-Root` = project
  .in(file("."))
  .enablePlugins(ScalaUnidocPlugin)
  .settings(
    ScalaUnidoc / unidoc / unidocProjectFilter := {
      inAnyProject -- inProjects(benchmarks, `ReactiveMongo-Core`)
    },
    mimaPreviousArtifacts := Set.empty,
    publishArtifact := false,
    publishTo := None,
    publishLocal := {},
    publish := {}
  )
  .aggregate(`ReactiveMongo-Core`, `ReactiveMongo`, `ReactiveMongo-Test`)

lazy val benchmarks = (project in file("benchmarks"))
  .enablePlugins(JmhPlugin)
  .settings(
    Compiler.settings ++ Seq(
      libraryDependencies += organization.value % "reactivemongo-shaded" % version.value
    )
  )
  .dependsOn(`ReactiveMongo`)
