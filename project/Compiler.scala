import sbt._
import sbt.Keys._

object Compiler {

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
      "-encoding",
      "UTF-8",
      "-unchecked",
      "-deprecation",
      "-feature",
      "-language:higherKinds"
    ),
    scalacOptions ++= {
      if (scalaBinaryVersion.value == "2.11") {
        Seq.empty[String]
      } else {
        Seq("-Xfatal-warnings")
      }
    },
    scalacOptions ++= {
      if (scalaBinaryVersion.value startsWith "2.") {
        Seq(
          "-Xlint",
          "-g:vars"
        )
      } else Seq.empty
    },
    scalacOptions ++= {
      val sv = scalaBinaryVersion.value

      if (sv == "2.12") {
        Seq(
          "-target:jvm-1.8",
          "-Xmax-classfile-name",
          "128",
          "-Ywarn-numeric-widen",
          "-Ywarn-dead-code",
          "-Ywarn-value-discard",
          "-Ywarn-infer-any",
          "-Ywarn-unused",
          "-Ywarn-unused-import",
          "-Xlint:missing-interpolator",
          "-Ywarn-macros:after"
        )
      } else if (sv == "2.11") {
        Seq(
          "-target:jvm-1.8",
          "-Xmax-classfile-name",
          "128",
          "-Yopt:_",
          "-Ydead-code",
          "-Yclosure-elim",
          "-Yconst-opt"
        )
      } else if (sv == "2.13") {
        Seq(
          "-release",
          "8",
          "-explaintypes",
          "-Werror",
          "-Wnumeric-widen",
          "-Wdead-code",
          "-Wvalue-discard",
          "-Wextra-implicit",
          "-Wmacros:after",
          "-Wunused"
        )
      } else {
        Seq("-Wunused:all", "-language:implicitConversions")
      }
    },
    Compile / console / scalacOptions ~= {
      _.filterNot(o => o.startsWith("-X") || o.startsWith("-Y"))
    },
    Test / scalacOptions ~= {
      _.filterNot(_ == "-Xfatal-warnings")
    },
    //
    Compile / scalacOptions ++= {
      val v = scalaBinaryVersion.value

      val mongo30eol = "MongoDB\\ 3\\.0\\ EOL\\ reached\\ by\\ Feb\\ 2018"
      val rightBiaised = "Either.*\\ right-biased"

      if (v == "2.11") {
        Seq.empty
      } else {
        Seq(
          s"-Wconf:cat=deprecation&msg=($mongo30eol|$rightBiaised|package\\ nio):s",
          "-Wconf:msg=.*nowarn.*\\ annotation.*:s",
          "-Wconf:msg=Implicit\\ parameters.*:s"
        )
      }
    },
    Compile / doc / scalacOptions ~= {
      _.filterNot(excludeOpt)
    },
    Compile / console / scalacOptions ~= {
      _.filterNot(excludeOpt)
    },
    Test / console / scalacOptions ~= {
      _.filterNot(excludeOpt)
    },
    Test / console / scalacOptions += "-Yrepl-class-based",
    Test / scalacOptions ++= {
      if (scalaBinaryVersion.value == "2.11") {
        Seq.empty
      } else {
        Seq(
          "-Wconf:src=.*test/.*&msg=.*type\\ was\\ inferred.*(Any|Object).*:s"
        )
      }
    }
  )

  private lazy val excludeOpt: String => Boolean = { opt =>
    (opt.startsWith("-X") && opt != "-Xmax-classfile-name") ||
    opt.startsWith("-Y") || opt.startsWith("-W") || opt.startsWith("-P")
  }
}
