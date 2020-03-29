import sbt._
import sbt.Keys._

object Compiler {
  private val silencerVersion = Def.setting[String] {
    if (scalaBinaryVersion.value == "2.11") "1.4.4"
    else "1.6.0"
  }

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
    unmanagedSourceDirectories in Compile ++= {
      unmanaged(scalaVersion.value, (sourceDirectory in Compile).value)
    },
    unmanagedSourceDirectories in Test ++= {
      unmanaged(scalaVersion.value, (sourceDirectory in Test).value)
    },
    scalacOptions ++= Seq(
      "-encoding", "UTF-8",
      "-unchecked",
      "-deprecation",
      "-feature",
      //"-Xfatal-warnings",
      "-Xlint",
      "-Ywarn-numeric-widen",
      "-Ywarn-dead-code",
      "-Ywarn-value-discard",
      "-g:vars"
    ),
    scalacOptions in Compile ++= {
      val ver = scalaBinaryVersion.value

      if (ver == "2.12") {
        Seq("-Ywarn-macros:after")
      } else if (ver != "2.11") {
        Nil
      } else Seq(
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
    scalacOptions in Compile ++= {
      val v = scalaBinaryVersion.value

      if (v == "2.13") {
        Nil
      } else {
        // TODO: Remove
        val m26 = "MongoDB\\ 2\\.6\\ EOL\\ reached\\ by\\ Oct\\ 2016"
        val m3 = "MongoDB\\ 3\\.0\\ EOL\\ reached\\ by\\ Feb\\ 2018"

        // Driver
        val internal = ".*Internal:\\ will\\ be\\ made\\ private.*"
        val cmd = "Will\\ be\\ removed;\\ See\\ `Command`"
        val repl = "Will\\ be\\ replaced\\ by\\ `reactivemongo.*"
        val ns1 = ".*in\\ package\\ nodeset.*is\\ deprecated.*"
        val ns2 = ".*class\\ NodeSet.*;NodeSetInfo\\ is\\ deprecated.*"
        val bcmd = ".*in\\ package\\ bson.*"
        val ncc = ".*No\\ longer\\ a\\ ReactiveMongo\\ case\\ class.*"
        val dc = ".*Command\\ in\\ package\\ commands\\ is\\ deprecated.*"
        val auth = ".*Use\\ SCRAM\\ or\\ X509.*"
        val driver = s".*Unused.*;$internal;$cmd;$repl;$ns1;$ns2;$bcmd;$ncc;$dc;$auth"

        // BSON
        val bll = ".*in\\ package\\ lowlevel\\ is\\ deprecated.*"
        val bxn = ".*ExtendedNumeric\\ is\\ deprecated.*"
        val useBison = ".*Use\\ reactivemongo-bson-api.*"
        val bson = s"$bll;$bxn;$useBison"

        val macros = ".*value\\ macro.*\\ is never used"

        Seq(
          "-Ywarn-infer-any",
          "-Ywarn-unused",
          "-Ywarn-unused-import",
          "-Xlint:missing-interpolator",
          s"-P:silencer:globalFilters=$bson;$driver;$m26;$m3;$macros"
        )
      }
    },
    scalacOptions in (Compile, console) ~= {
      _.filterNot(excludeOpt)
    },
    scalacOptions in (Test, console) ~= {
      _.filterNot(excludeOpt)
    },
    scalacOptions in (Test, console) += "-Yrepl-class-based",
  )

  private lazy val excludeOpt: String => Boolean = { opt =>
    opt.startsWith("-X") || opt.startsWith("-Y") ||
    opt.startsWith("-P:silencer")
  }
}
