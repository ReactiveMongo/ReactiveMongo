import sbt._
import sbt.Keys._

object Compiler {
  private val silencerVersion = Def.setting[String] {
    if (scalaVersion.value startsWith "2.10.") "1.2.1"
    else "1.4.1"
  }

  private def unmanaged(ver: String, base: File): File = 
    CrossVersion.partialVersion(ver) match {
      case Some((2, n)) if n >= 13 => base / "scala-2.13+"
      case _                       => base / "scala-2.13-"
    }


  val settings = Seq(
    unmanagedSourceDirectories in Compile += {
      unmanaged(scalaVersion.value, (sourceDirectory in Compile).value)
    },
    unmanagedSourceDirectories in Test += {
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
    libraryDependencies ++= {
      if (scalaVersion.value startsWith "2.10.") Nil
      else Seq(
        compilerPlugin(
          "com.github.ghik" %% "silencer-plugin" % silencerVersion.value),
        "com.github.ghik" %% "silencer-lib" % silencerVersion.value % Provided)
    },
    scalacOptions in Compile ++= {
      val v = scalaVersion.value

      if (v.startsWith("2.10.") || v.startsWith("2.13.")) {
        Nil
      } else {
        val m26 = "MongoDB\\ 2\\.6\\ EOL\\ reached\\ by\\ Oct\\ 2016"
        val m3 = "MongoDB\\ 3\\.0\\ EOL\\ reached\\ by\\ Feb\\ 2018"

        // Driver
        val internal = ".*Internal:\\ will\\ be\\ made\\ private.*"
        val cmd = "Will\\ be\\ removed;\\ See\\ `Command`"
        val repl = "Will\\ be\\ replaced\\ by\\ `reactivemongo.*"
        val ns1 = ".*in\\ package\\ nodeset.*is\\ deprecated.*"
        val ns2 = ".*class\\ NodeSet.*;NodeSetInfo\\ is\\ deprecated.*"
        val bcmd = ".*in\\ package\\ bson.*"
        val driver = s"$internal;$cmd;$repl;$ns1;$ns2;$bcmd"

        // BSON
        val bll = ".*in\\ package\\ lowlevel\\ is\\ deprecated.*"
        val bxn = ".*ExtendedNumeric\\ is\\ deprecated.*"
        val bson = s"$bll;$bxn"

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
    scalacOptions in Compile ++= {
      val v = scalaVersion.value

      if (!v.startsWith("2.12.") && !v.startsWith("2.13.")) {
        Seq("-target:jvm-1.6")
      } else Seq("-target:jvm-1.8")
    },
    scalacOptions in (Compile, console) ~= {
      _.filterNot(excludeOpt)
    },
    scalacOptions in (Test, console) ~= {
      _.filterNot(excludeOpt)
    },
    scalacOptions in (Test, console) += "-Yrepl-class-based",
    scalacOptions in Compile := {
      val opts = (scalacOptions in Compile).value

      if (scalaVersion.value != "2.10.7") opts
      else {
        opts.filter(_ != "-Ywarn-unused-import")
      }
    }
  )

  private lazy val excludeOpt: String => Boolean = { opt =>
    opt.startsWith("-X") || opt.startsWith("-Y") ||
    opt.startsWith("-P:silencer")
  }
}
