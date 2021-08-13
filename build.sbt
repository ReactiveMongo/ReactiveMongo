lazy val `ReactiveMongo-Core` = project.in(file("core")).settings(
  Compile / sourceDirectories ++= Seq(
    (Compile / sourceDirectory).value / "scala-2.11+"),
  libraryDependencies ++= {
    val deps = Dependencies.shaded.value

    ("org.reactivemongo" %% "reactivemongo-bson-api" % version.
      value) +: deps ++: Seq(
        "com.github.luben" % "zstd-jni" % "1.5.0-4",
        "org.xerial.snappy" % "snappy-java" % "1.1.8.4",
        Dependencies.specs.value
      )
  },
  mimaBinaryIssueFilters ++= {
    import com.typesafe.tools.mima.core._

    val mtp = ProblemFilters.exclude[MissingTypesProblem](_)

    Seq(
      mtp("reactivemongo.core.protocol.ResponseDecoder"),
      mtp("reactivemongo.core.actors.MongoDBSystem$OperationHandler"),
      mtp("reactivemongo.core.netty.ChannelFactory"),
      mtp("reactivemongo.core.protocol.MongoHandler"),
      mtp("reactivemongo.core.protocol.ResponseFrameDecoder"),
      mtp("reactivemongo.core.protocol.RequestEncoder")
    )
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
    mimaPreviousArtifacts := Set.empty,
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
