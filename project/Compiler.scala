import sbt._
import sbt.Keys._

object Compiler {
  private val silencerVersion = Def.setting[String]("1.7.6")

  private def unmanaged(ver: String, base: File): Seq[File] =
    CrossVersion.partialVersion(ver) match {
      case Some((2, 10)) =>
        Seq(base / "scala-2.13-")

      case Some((2, n)) if n < 13 =>
        Seq(base / "scala-2.13-", base / "scala-2.11+")

      case _ =>
        Seq(base / "scala-2.13+", base / "scala-2.11+")

    }

  val settings = Seq(
    Compile / unmanagedSourceDirectories ++= {
      unmanaged(scalaVersion.value, (Compile / sourceDirectory).value)
    },
    Test / unmanagedSourceDirectories ++= {
      unmanaged(scalaVersion.value, (Test / sourceDirectory).value)
    },
    scalacOptions ++= Seq(
      "-encoding", "UTF-8",
      "-unchecked",
      "-deprecation",
      "-feature",
      "-language:higherKinds",
      "-Xfatal-warnings",
      "-Xlint",
      "-Ywarn-numeric-widen",
      "-Ywarn-dead-code",
      "-Ywarn-value-discard",
      "-g:vars"
    ),
    Compile / scalacOptions ++= {
      val ver = scalaBinaryVersion.value

      if (ver == "2.12") {
        Seq(
          "-Xmax-classfile-name", "128",
          "-Ywarn-macros:after")
      } else if (ver != "2.11") { // 2.13
        Seq("-Wmacros:after")
      } else Seq(
        "-Xmax-classfile-name", "128",
        "-Yconst-opt",
        "-Yclosure-elim",
        "-Ydead-code",
        "-Yopt:_"
      )
    },
    libraryDependencies ++= Seq(
      compilerPlugin(
        ("com.github.ghik" %% "silencer-plugin" % silencerVersion.value).
          cross(CrossVersion.full)),
      ("com.github.ghik" %% "silencer-lib" % silencerVersion.
        value % Provided).cross(CrossVersion.full)),
    Compile / scalacOptions ++= {
      val v = scalaBinaryVersion.value

      val mongo30eol = "MongoDB\\ 3\\.0\\ EOL\\ reached\\ by\\ Feb\\ 2018"
      val rightBiaised = "Either\\ is\\ now\\ right-biased"

      val silencer = s"-P:silencer:globalFilters=$mongo30eol;$rightBiaised"

      if (v == "2.13") {
        Seq(silencer)
      } else {
        Seq(
          "-Ywarn-infer-any",
          "-Ywarn-unused",
          "-Ywarn-unused-import",
          "-Xlint:missing-interpolator",
          silencer
        )
      }
    },
    Compile / console / scalacOptions ~= {
      _.filterNot(excludeOpt)
    },
    Test / console / scalacOptions ~= {
      _.filterNot(excludeOpt)
    },
    Test / console / scalacOptions += "-Yrepl-class-based",
  )

  private lazy val excludeOpt: String => Boolean = { opt =>
    opt.startsWith("-X") || opt.startsWith("-Y") ||
    opt.startsWith("-P:silencer")
  }
}
